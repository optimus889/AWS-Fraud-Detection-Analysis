import os
import json
import base64
import uuid
import boto3
from datetime import datetime

# AWS clients
runtime = boto3.client("sagemaker-runtime")
s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

# Environment variables
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME", "sagemaker-xgboost-2026-03-13-23-05-26-528")
PREDICTION_BUCKET = os.environ.get("PREDICTION_BUCKET", "finalproject-fraud-detection")
PREDICTION_PREFIX = os.environ.get("PREDICTION_PREFIX", "predictions/realtime")
ENDPOINT_BATCH_SIZE = int(os.environ.get("ENDPOINT_BATCH_SIZE", "500"))
SAVE_ONE_FILE_PER_BATCH = os.environ.get("SAVE_ONE_FILE_PER_BATCH", "true").lower() == "true"
THRESHOLD = float(os.environ.get("THRESHOLD", "0.5"))

# CloudWatch custom metric namespace
CW_NAMESPACE = os.environ.get("CW_NAMESPACE", "FraudDetection")


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def publish_cloudwatch_metrics(metric_data):
    """
    Publish custom CloudWatch metrics in batches.
    CloudWatch put_metric_data supports up to 20 metrics per call.
    """
    if not metric_data:
        return

    for batch in chunked(metric_data, 20):
        cloudwatch.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=batch
        )


def build_feature_vector(record):
    """
    Build the feature vector in the exact same order as the offline training data.

    Feature order:
    step, amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest,
    type_CASH_IN, type_CASH_OUT, type_DEBIT, type_PAYMENT, type_TRANSFER
    """
    txn_type = str(record.get("type", "")).upper()

    type_cash_in = 1 if txn_type == "CASH_IN" else 0
    type_cash_out = 1 if txn_type == "CASH_OUT" else 0
    type_debit = 1 if txn_type == "DEBIT" else 0
    type_payment = 1 if txn_type == "PAYMENT" else 0
    type_transfer = 1 if txn_type == "TRANSFER" else 0

    return [
        safe_int(record.get("step", 0)),
        safe_float(record.get("amount", 0.0)),
        safe_float(record.get("oldbalanceOrg", 0.0)),
        safe_float(record.get("newbalanceOrig", 0.0)),
        safe_float(record.get("oldbalanceDest", 0.0)),
        safe_float(record.get("newbalanceDest", 0.0)),
        type_cash_in,
        type_cash_out,
        type_debit,
        type_payment,
        type_transfer,
    ]


def build_csv_payload(feature_rows):
    return "\n".join(
        ",".join(map(str, row))
        for row in feature_rows
    )


def parse_batch_prediction_result(result_text):
    result_text = result_text.strip()

    if not result_text:
        return []

    try:
        parsed = json.loads(result_text)
        if isinstance(parsed, dict) and "predictions" in parsed:
            predictions = parsed["predictions"]
            scores = []
            for item in predictions:
                if isinstance(item, dict) and "score" in item:
                    scores.append(float(item["score"]))
                else:
                    scores.append(float(item))
            return scores
    except Exception:
        pass

    if "\n" in result_text:
        return [float(x.strip()) for x in result_text.split("\n") if x.strip()]

    if "," in result_text:
        return [float(x.strip()) for x in result_text.split(",") if x.strip()]

    return [float(result_text)]


def invoke_endpoint_batch(feature_rows):
    payload = build_csv_payload(feature_rows)

    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="text/csv",
        Body=payload
    )

    result_text = response["Body"].read().decode("utf-8").strip()
    scores = parse_batch_prediction_result(result_text)

    return scores

def save_batch_prediction_results(output_records):
    raw_ts = output_records[0].get("timestamp") or output_records[0].get("processed_at")

    if raw_ts:
        event_time = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    else:
        event_time = datetime.utcnow()

    processing_time = datetime.utcnow()
    file_id = uuid.uuid4().hex

    s3_key = (
        f"{PREDICTION_PREFIX}/"
        f"year={event_time.year}/month={event_time.month:02d}/day={event_time.day:02d}/"
        f"batch_{processing_time.strftime('%Y%m%dT%H%M%S')}_{file_id}.jsonl"
    )

    body = "\n".join(json.dumps(record) for record in output_records)

    s3.put_object(
        Bucket=PREDICTION_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    return s3_key

def save_prediction_result(output_record):
    raw_ts = output_record.get("timestamp") or output_record.get("processed_at")

    if raw_ts:
        event_time = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    else:
        event_time = datetime.utcnow()

    file_id = uuid.uuid4().hex

    s3_key = (
        f"{PREDICTION_PREFIX}/"
        f"year={event_time.year}/month={event_time.month:02d}/day={event_time.day:02d}/"
        f"record_{file_id}.json"
    )

    s3.put_object(
        Bucket=PREDICTION_BUCKET,
        Key=s3_key,
        Body=json.dumps(output_record).encode("utf-8"),
        ContentType="application/json"
    )

    return s3_key


def lambda_handler(event, context):
    valid_records = []
    decode_errors = []

    total_input_records = len(event.get("Records", []))
    endpoint_error_count = 0
    prediction_mismatch_count = 0
    s3_write_success_count = 0

    # Step 1: Decode Kinesis records
    for item in event.get("Records", []):
        try:
            raw_data = base64.b64decode(item["kinesis"]["data"]).decode("utf-8")
            record = json.loads(raw_data)
            valid_records.append(record)
        except Exception as e:
            decode_errors.append({
                "error": f"decode_or_parse_error: {str(e)}"
            })

    if not valid_records:
        publish_cloudwatch_metrics([
            {
                "MetricName": "InputRecordCount",
                "Value": total_input_records,
                "Unit": "Count"
            },
            {
                "MetricName": "ValidRecordCount",
                "Value": 0,
                "Unit": "Count"
            },
            {
                "MetricName": "DecodeErrorCount",
                "Value": len(decode_errors),
                "Unit": "Count"
            }
        ])

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No valid records to process",
                "errors": decode_errors
            })
        }

    all_results = []
    batch_s3_keys = []
    all_scores = []
    fraud_prediction_count = 0

    # Step 2: Invoke endpoint in batches inside one Lambda execution
    for batch_number, record_batch in enumerate(chunked(valid_records, ENDPOINT_BATCH_SIZE), start=1):
        feature_rows = [build_feature_vector(record) for record in record_batch]

        try:
            scores = invoke_endpoint_batch(feature_rows)
        except Exception as e:
            endpoint_error_count += 1

            publish_cloudwatch_metrics([
                {
                    "MetricName": "EndpointInvocationErrorCount",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "EndpointName", "Value": ENDPOINT_NAME}
                    ]
                }
            ])

            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": "Endpoint batch invocation failed",
                    "error": str(e),
                    "batch_number": batch_number,
                    "record_count": len(record_batch)
                })
            }

        if len(scores) != len(record_batch):
            prediction_mismatch_count += 1

            publish_cloudwatch_metrics([
                {
                    "MetricName": "PredictionMismatchCount",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "EndpointName", "Value": ENDPOINT_NAME}
                    ]
                }
            ])

            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": "Prediction count does not match input count",
                    "batch_number": batch_number,
                    "input_count": len(record_batch),
                    "prediction_count": len(scores)
                })
            }

        output_records = []

        for record, features, score in zip(record_batch, feature_rows, scores):
            predicted_label = 1 if float(score) >= THRESHOLD else 0
            endpoint_payload = ",".join(map(str, features))

            all_scores.append(float(score))
            fraud_prediction_count += predicted_label

            output_record = {
                "transaction_id": record.get("transaction_id"),
                "timestamp": record.get("timestamp"),
                "location": record.get("location"),
                "feature_version": record.get("feature_version", "v1"),
                "type": record.get("type"),
                "step": record.get("step"),
                "amount": record.get("amount"),
                "oldbalanceorg": record.get("oldbalanceOrg"),
                "newbalanceorig": record.get("newbalanceOrig"),
                "oldbalancedest": record.get("oldbalanceDest"),
                "newbalancedest": record.get("newbalanceDest"),
                "actual_isfraud": record.get("actual_isFraud"),
                "predicted_score": float(score),
                "predicted_label": predicted_label,
                "threshold": THRESHOLD,
                "endpoint_name": ENDPOINT_NAME,
                "endpoint_payload": endpoint_payload,
                "processed_at": datetime.utcnow().isoformat() + "Z"
            }
            output_records.append(output_record)

        # Step 3: Save results to S3
        if SAVE_ONE_FILE_PER_BATCH:
            batch_s3_key = save_batch_prediction_results(output_records)
            batch_s3_keys.append(batch_s3_key)
            s3_write_success_count += 1

            for output_record in output_records:
                all_results.append({
                    "transaction_id": output_record["transaction_id"],
                    "predicted_score": output_record["predicted_score"],
                    "predicted_label": output_record["predicted_label"],
                    "s3_key": batch_s3_key
                })
        else:
            for output_record in output_records:
                s3_key = save_prediction_result(output_record)
                s3_write_success_count += 1
                all_results.append({
                    "transaction_id": output_record["transaction_id"],
                    "predicted_score": output_record["predicted_score"],
                    "predicted_label": output_record["predicted_label"],
                    "s3_key": s3_key
                })

    processed_count = len(all_results)
    avg_predicted_score = sum(all_scores) / len(all_scores) if all_scores else 0.0
    fraud_prediction_rate = (
        fraud_prediction_count / processed_count if processed_count > 0 else 0.0
    )

    # Step 4: Publish summary metrics to CloudWatch
    publish_cloudwatch_metrics([
        {
            "MetricName": "InputRecordCount",
            "Value": total_input_records,
            "Unit": "Count"
        },
        {
            "MetricName": "ValidRecordCount",
            "Value": len(valid_records),
            "Unit": "Count"
        },
        {
            "MetricName": "ProcessedRecordCount",
            "Value": processed_count,
            "Unit": "Count"
        },
        {
            "MetricName": "DecodeErrorCount",
            "Value": len(decode_errors),
            "Unit": "Count"
        },
        {
            "MetricName": "FraudPredictionCount",
            "Value": fraud_prediction_count,
            "Unit": "Count"
        },
        {
            "MetricName": "FraudPredictionRate",
            "Value": fraud_prediction_rate,
            "Unit": "None"
        },
        {
            "MetricName": "AvgPredictedScore",
            "Value": avg_predicted_score,
            "Unit": "None"
        },
        {
            "MetricName": "EndpointInvocationErrorCount",
            "Value": endpoint_error_count,
            "Unit": "Count",
            "Dimensions": [
                {"Name": "EndpointName", "Value": ENDPOINT_NAME}
            ]
        },
        {
            "MetricName": "PredictionMismatchCount",
            "Value": prediction_mismatch_count,
            "Unit": "Count",
            "Dimensions": [
                {"Name": "EndpointName", "Value": ENDPOINT_NAME}
            ]
        },
        {
            "MetricName": "S3WriteSuccessCount",
            "Value": s3_write_success_count,
            "Unit": "Count"
        }
    ])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed_count": processed_count,
            "fraud_prediction_count": fraud_prediction_count,
            "fraud_prediction_rate": fraud_prediction_rate,
            "avg_predicted_score": avg_predicted_score,
            "endpoint_batch_size": ENDPOINT_BATCH_SIZE,
            "save_one_file_per_batch": SAVE_ONE_FILE_PER_BATCH,
            "batch_s3_keys": batch_s3_keys,
            "errors": decode_errors,
            "results": all_results
        })
    }