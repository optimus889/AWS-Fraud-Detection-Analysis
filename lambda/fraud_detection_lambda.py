import json
import os
import base64
import boto3
import logging
from datetime import datetime

runtime = boto3.client("sagemaker-runtime")
s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ["BUCKET_NAME"]
ENDPOINT_NAME = os.environ["ENDPOINT_NAME"]
PREDICTION_PREFIX = os.environ.get("PREDICTION_PREFIX", "predictions/")

MERCHANT_MAP = {
    "Amazon": 0, "Walmart": 1, "Apple Store": 2, "Best Buy": 3,
    "Target": 4, "Starbucks": 5, "eBay": 6, "Uber": 7, "Netflix": 8
}

LOCATION_MAP = {
    "New York": 0, "Boston": 1, "San Francisco": 2, "Chicago": 3,
    "Los Angeles": 4, "Seattle": 5, "Dallas": 6, "Miami": 7
}

PAYMENT_MAP = {
    "Credit Card": 0, "Debit Card": 1, "Online Payment": 2, "Mobile Pay": 3
}

def put_custom_metrics(prediction_score, fraud_flag):
    cloudwatch.put_metric_data(
        Namespace="FraudDetection",
        MetricData=[
            {
                "MetricName": "PredictionScore",
                "Dimensions": [
                    {"Name": "Environment", "Value": "Prod"},
                    {"Name": "ModelEndpoint", "Value": ENDPOINT_NAME}
                ],
                "Value": float(prediction_score),
                "Unit": "None"
            },
            {
                "MetricName": "FraudCount",
                "Dimensions": [
                    {"Name": "Environment", "Value": "Prod"},
                    {"Name": "ModelEndpoint", "Value": ENDPOINT_NAME}
                ],
                "Value": int(fraud_flag),
                "Unit": "Count"
            },
            {
                "MetricName": "PredictionCount",
                "Dimensions": [
                    {"Name": "Environment", "Value": "Prod"},
                    {"Name": "ModelEndpoint", "Value": ENDPOINT_NAME}
                ],
                "Value": 1,
                "Unit": "Count"
            }
        ]
    )
    logger.info("CloudWatch metrics published successfully")

def encode_transaction(tx):
    ts = datetime.fromisoformat(tx["timestamp"].replace("Z", ""))

    merchant_enc = MERCHANT_MAP.get(tx["merchant"], -1)
    location_enc = LOCATION_MAP.get(tx["location"], -1)
    payment_enc = PAYMENT_MAP.get(tx["payment_method"], -1)

    if -1 in [merchant_enc, location_enc, payment_enc]:
        raise ValueError(
            f"Unknown categorical value: merchant={tx.get('merchant')}, "
            f"location={tx.get('location')}, payment_method={tx.get('payment_method')}"
        )

    return [
        float(tx["amount"]),
        ts.hour,
        ts.day,
        ts.month,
        merchant_enc,
        location_enc,
        payment_enc
    ]

def invoke_model(features):
    payload = ",".join(map(str, features))
    logger.info("Invoking endpoint %s with payload: %s", ENDPOINT_NAME, payload)

    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="text/csv",
        Body=payload
    )

    result = response["Body"].read().decode("utf-8").strip()
    logger.info("Raw model response: %s", result)
    return result

def parse_prediction(prediction_result):
    try:
        data = json.loads(prediction_result)
    except json.JSONDecodeError:
        return float(prediction_result)

    if isinstance(data, (int, float)):
        return float(data)

    if isinstance(data, list) and len(data) > 0:
        return float(data[0])

    if isinstance(data, dict):
        if "predictions" in data:
            preds = data["predictions"]
            if isinstance(preds, list) and len(preds) > 0:
                first = preds[0]
                if isinstance(first, dict) and "score" in first:
                    return float(first["score"])
                return float(first)
        if "score" in data:
            return float(data["score"])

    raise ValueError(f"Unsupported prediction format: {prediction_result}")

def save_prediction(original_tx, prediction_score, fraud_flag):
    if not PREDICTION_PREFIX.endswith("/"):
        prefix = PREDICTION_PREFIX + "/"
    else:
        prefix = PREDICTION_PREFIX

    timestamp_str = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    key = f"{prefix}prediction-{timestamp_str}.json"

    output = {
        "transaction_id": original_tx["transaction_id"],
        "timestamp": original_tx["timestamp"],
        "amount": original_tx["amount"],
        "merchant": original_tx["merchant"],
        "location": original_tx["location"],
        "payment_method": original_tx["payment_method"],
        "actual_fraud": original_tx.get("fraud"),
        "prediction_score": prediction_score,
        "predicted_fraud": fraud_flag
    }

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(output),
        ContentType="application/json"
    )

    logger.info("Saved prediction to s3://%s/%s", BUCKET, key)
    return key

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    results = []
    failures = []

    for record in event.get("Records", []):
        sequence_number = record.get("kinesis", {}).get("sequenceNumber")
        try:
            payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            logger.info("Decoded payload: %s", payload)

            tx = json.loads(payload)
            features = encode_transaction(tx)
            prediction_result = invoke_model(features)
            prediction_score = parse_prediction(prediction_result)
            fraud_flag = 1 if prediction_score > 0.8 else 0

            put_custom_metrics(prediction_score, fraud_flag)
            s3_key = save_prediction(tx, prediction_score, fraud_flag)

            results.append({
                "transaction_id": tx["transaction_id"],
                "prediction_score": prediction_score,
                "predicted_fraud": fraud_flag,
                "s3_key": s3_key
            })

        except Exception as e:
            logger.error("Failed record sequence=%s error=%s", sequence_number, str(e), exc_info=True)
            if sequence_number:
                failures.append({"itemIdentifier": sequence_number})

    logger.info("Processed results: %s", json.dumps(results))
    logger.info("Failures: %s", json.dumps(failures))

    return {
        "batchItemFailures": failures
    }