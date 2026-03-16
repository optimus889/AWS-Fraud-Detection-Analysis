import json
import time
import random
import uuid
import boto3
import pandas as pd
from io import TextIOWrapper
from datetime import datetime, timedelta

# =========================
# Basic configuration
# =========================
REGION = "us-east-1"
BUCKET_NAME = "finalproject-fraud-detection"
CSV_S3_KEY = "raw/PS_20174392719_1491204439457_log.csv"
STREAM_NAME = "fraud-stream"

# =========================
# Demo generation settings
# =========================
TOTAL_RECORDS = 100000               # Generate 100,000 records
FRAUD_RATIO = 0.20                   # 20% fraud ratio
CHUNK_SIZE = 200000                  # Read source CSV faster
MAX_CANDIDATES = None                # Read until pools are large enough
MAX_FRAUD_POOL = 30000               # Fraud source pool kept in memory
MAX_NORMAL_POOL = 120000             # Normal source pool kept in memory

# =========================
# Streaming speed settings
# =========================
SHUFFLE_RECORDS = True
SEND_DELAY_SECONDS = 0.2             # Send delay every 0.2 seconds
KINESIS_BATCH_SIZE = 200             # Kinesis put_records max = 200
PRINT_EVERY_BATCHES = 20             # Print progress every 20 batches

# =========================
# Location distribution
# =========================
LOCATION_WEIGHTS = {
    "New York": 20,
    "Boston": 12,
    "San Francisco": 10,
    "Los Angeles": 10,
    "Chicago": 9,
    "Seattle": 8,
    "Dallas": 8,
    "Miami": 7,
    "Atlanta": 6,
    "Washington DC": 5,
    "Houston": 5
}

# =========================
# AWS clients
# =========================
s3 = boto3.client("s3", region_name=REGION)
kinesis = boto3.client("kinesis", region_name=REGION)

locations = list(LOCATION_WEIGHTS.keys())
weights = list(LOCATION_WEIGHTS.values())


def random_location():
    return random.choices(locations, weights=weights, k=1)[0]


def random_event_time():
    now = datetime.now()
    offset_seconds = random.randint(-120, 120)
    return (now + timedelta(seconds=offset_seconds)).isoformat() + "Z"


def collect_balanced_pools_from_s3():
    """
    Read the CSV from S3 in chunks and build two pools:
    one for fraud records and one for normal records.
    """
    print("Start reading CSV from S3 for balanced demo generation...")

    response = s3.get_object(Bucket=BUCKET_NAME, Key=CSV_S3_KEY)
    body = response["Body"]

    usecols = [
        "step",
        "type",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud"
    ]

    dtype_map = {
        "step": "int32",
        "type": "string",
        "amount": "float32",
        "oldbalanceOrg": "float32",
        "newbalanceOrig": "float32",
        "oldbalanceDest": "float32",
        "newbalanceDest": "float32",
        "isFraud": "int8"
    }

    fraud_pool = []
    normal_pool = []
    seen = 0

    text_stream = TextIOWrapper(body, encoding="utf-8")

    for chunk in pd.read_csv(
        text_stream,
        usecols=usecols,
        dtype=dtype_map,
        chunksize=CHUNK_SIZE
    ):
        for row in chunk.itertuples(index=False):
            record = {
                "step": int(row.step),
                "type": str(row.type),
                "amount": round(float(row.amount), 2),
                "oldbalanceOrg": round(float(row.oldbalanceOrg), 2),
                "newbalanceOrig": round(float(row.newbalanceOrig), 2),
                "oldbalanceDest": round(float(row.oldbalanceDest), 2),
                "newbalanceDest": round(float(row.newbalanceDest), 2),
                "actual_isFraud": int(row.isFraud)
            }

            seen += 1

            if record["actual_isFraud"] == 1:
                if len(fraud_pool) < MAX_FRAUD_POOL:
                    fraud_pool.append(record)
            else:
                if len(normal_pool) < MAX_NORMAL_POOL:
                    normal_pool.append(record)

        if seen % 200000 == 0:
            print(
                f"Scanned {seen} rows | "
                f"fraud_pool={len(fraud_pool)} | "
                f"normal_pool={len(normal_pool)}"
            )

        if len(fraud_pool) >= MAX_FRAUD_POOL and len(normal_pool) >= MAX_NORMAL_POOL:
            print("Reached target pool sizes, stop reading early.")
            break

        if MAX_CANDIDATES is not None and seen >= MAX_CANDIDATES:
            print(f"Reached MAX_CANDIDATES={MAX_CANDIDATES}, stop reading.")
            break

    print(f"Finished scanning {seen} rows.")
    print(f"Fraud pool size: {len(fraud_pool)}")
    print(f"Normal pool size: {len(normal_pool)}")

    if len(fraud_pool) == 0:
        raise ValueError("No fraud samples found in source data.")
    if len(normal_pool) == 0:
        raise ValueError("No normal samples found in source data.")

    return fraud_pool, normal_pool


def build_demo_records(fraud_pool, normal_pool, total_records, fraud_ratio):
    fraud_target = int(total_records * fraud_ratio)
    normal_target = total_records - fraud_target

    fraud_records = random.choices(fraud_pool, k=fraud_target)
    normal_records = random.choices(normal_pool, k=normal_target)

    demo_records = fraud_records + normal_records

    if SHUFFLE_RECORDS:
        random.shuffle(demo_records)

    return demo_records


def build_kinesis_entry(item, idx):
    transaction_id = f"TX-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}-{idx}"

    record = {
        "transaction_id": transaction_id,
        "timestamp": random_event_time(),
        "feature_version": "v1",
        "step": item["step"],
        "type": item["type"],
        "amount": item["amount"],
        "oldbalanceOrg": item["oldbalanceOrg"],
        "newbalanceOrig": item["newbalanceOrig"],
        "oldbalanceDest": item["oldbalanceDest"],
        "newbalanceDest": item["newbalanceDest"],
        "location": random_location(),
        "actual_isFraud": item["actual_isFraud"]
    }

    return {
        "Data": json.dumps(record).encode("utf-8"),
        "PartitionKey": transaction_id
    }, record


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def send_records_to_kinesis(records):
    """
    Send records to Kinesis using put_records for much higher throughput.
    """
    print(f"Ready to send {len(records)} demo transactions to Kinesis stream: {STREAM_NAME}")

    fraud_count = 0
    normal_count = 0
    sent_count = 0
    batch_count = 0

    indexed_records = list(enumerate(records, start=1))

    for batch in chunked(indexed_records, KINESIS_BATCH_SIZE):
        entries = []
        batch_record_meta = []

        for idx, item in batch:
            entry, record = build_kinesis_entry(item, idx)
            entries.append(entry)
            batch_record_meta.append(record)

        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=entries
        )

        failed_count = response.get("FailedRecordCount", 0)
        if failed_count > 0:
            retry_entries = []
            retry_meta = []
            for entry, meta, result in zip(entries, batch_record_meta, response.get("Records", [])):
                if "ErrorCode" in result:
                    retry_entries.append(entry)
                    retry_meta.append(meta)

            if retry_entries:
                retry_response = kinesis.put_records(
                    StreamName=STREAM_NAME,
                    Records=retry_entries
                )
                retry_failed = retry_response.get("FailedRecordCount", 0)
                if retry_failed > 0:
                    raise RuntimeError(
                        f"Kinesis put_records retry still failed for {retry_failed} records"
                    )

        for record in batch_record_meta:
            if record["actual_isFraud"] == 1:
                fraud_count += 1
            else:
                normal_count += 1

        sent_count += len(batch)
        batch_count += 1

        if batch_count % PRINT_EVERY_BATCHES == 0 or sent_count == len(records):
            print(
                f"Sent {sent_count}/{len(records)} | "
                f"fraud_sent={fraud_count} | "
                f"normal_sent={normal_count} | "
                f"batches={batch_count}"
            )

        if SEND_DELAY_SECONDS > 0:
            time.sleep(SEND_DELAY_SECONDS)

    print("Streaming completed.")
    print(f"Final summary | fraud_sent={fraud_count} | normal_sent={normal_count}")


def main():
    start_time = time.time()

    fraud_pool, normal_pool = collect_balanced_pools_from_s3()
    demo_records = build_demo_records(
        fraud_pool=fraud_pool,
        normal_pool=normal_pool,
        total_records=TOTAL_RECORDS,
        fraud_ratio=FRAUD_RATIO
    )
    send_records_to_kinesis(demo_records)

    elapsed = time.time() - start_time
    print(f"Total elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
