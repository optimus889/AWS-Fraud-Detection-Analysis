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
RECORDS_PER_DAY = 100000
NUM_DAYS = 7
START_DATE = "2026-03-17"
CHUNK_SIZE = 200000
MAX_CANDIDATES = None
MAX_FRAUD_POOL = 30000
MAX_NORMAL_POOL = 120000

# =========================
# Fraud ratio settings
# =========================
# Option 1:
# Use fixed fraud ratios for each day.
# The length should be at least NUM_DAYS.
USE_AUTO_FRAUD_RATIOS = False
DAILY_FRAUD_RATIOS = [0.05, 0.08, 0.12, 0.18, 0.10, 0.22, 0.15]

# Option 2:
# Automatically generate daily fraud ratios.
# When USE_AUTO_FRAUD_RATIOS = True, the code will generate one ratio per day
# within the range [AUTO_FRAUD_MIN, AUTO_FRAUD_MAX].
AUTO_FRAUD_MIN = 0.05
AUTO_FRAUD_MAX = 0.25
AUTO_FRAUD_ROUND_DIGITS = 2

# Optional:
# Set a seed for reproducible fraud ratio generation.
# Set to None if you want different results in each run.
FRAUD_RATIO_RANDOM_SEED = 42

# =========================
# Streaming speed settings
# =========================
SHUFFLE_RECORDS = True
SEND_DELAY_SECONDS = 0.2
KINESIS_BATCH_SIZE = 200
PRINT_EVERY_BATCHES = 20

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


def random_event_time_for_day(day_start: datetime):
    """
    Generate a random timestamp within the specified day.
    The generated time will fall between 00:00:00 and 23:59:59.
    """
    offset_seconds = random.randint(0, 86399)
    return (day_start + timedelta(seconds=offset_seconds)).isoformat() + "Z"


def get_daily_fraud_ratios():
    """
    Return the fraud ratio list for all days.

    If USE_AUTO_FRAUD_RATIOS is True, generate a ratio for each day
    within the configured min/max range.

    Otherwise, use the manually defined DAILY_FRAUD_RATIOS.
    """
    if USE_AUTO_FRAUD_RATIOS:
        if FRAUD_RATIO_RANDOM_SEED is not None:
            random.seed(FRAUD_RATIO_RANDOM_SEED)

        ratios = [
            round(random.uniform(AUTO_FRAUD_MIN, AUTO_FRAUD_MAX), AUTO_FRAUD_ROUND_DIGITS)
            for _ in range(NUM_DAYS)
        ]
        return ratios

    if len(DAILY_FRAUD_RATIOS) < NUM_DAYS:
        raise ValueError("DAILY_FRAUD_RATIOS length must be >= NUM_DAYS")

    return DAILY_FRAUD_RATIOS[:NUM_DAYS]


def collect_balanced_pools_from_s3():
    """
    Read the source CSV from S3 and build two sample pools:
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
    """
    Build one day's demo records using the requested fraud ratio.
    """
    fraud_target = int(total_records * fraud_ratio)
    normal_target = total_records - fraud_target

    fraud_records = random.choices(fraud_pool, k=fraud_target)
    normal_records = random.choices(normal_pool, k=normal_target)

    demo_records = fraud_records + normal_records

    if SHUFFLE_RECORDS:
        random.shuffle(demo_records)

    return demo_records


def build_kinesis_entry(item, idx, event_time_str):
    """
    Convert one transaction record into a Kinesis put_records entry.
    """
    transaction_id = f"TX-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}-{idx}"

    record = {
        "transaction_id": transaction_id,
        "timestamp": event_time_str,
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
    """
    Yield a list in chunks of the specified size.
    """
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def send_records_to_kinesis_for_day(records, day_start, day_label):
    """
    Send one day's records to the Kinesis stream.
    """
    print(f"Ready to send {len(records)} demo transactions for {day_label} to Kinesis stream: {STREAM_NAME}")

    fraud_count = 0
    normal_count = 0
    sent_count = 0
    batch_count = 0

    indexed_records = list(enumerate(records, start=1))

    for batch in chunked(indexed_records, KINESIS_BATCH_SIZE):
        entries = []
        batch_record_meta = []

        for idx, item in batch:
            event_time_str = random_event_time_for_day(day_start)
            entry, record = build_kinesis_entry(item, idx, event_time_str)
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
                        f"Kinesis put_records retry still failed for {retry_failed} records on {day_label}"
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
                f"[{day_label}] Sent {sent_count}/{len(records)} | "
                f"fraud_sent={fraud_count} | "
                f"normal_sent={normal_count} | "
                f"batches={batch_count}"
            )

        if SEND_DELAY_SECONDS > 0:
            time.sleep(SEND_DELAY_SECONDS)

    print(f"{day_label} streaming completed.")
    print(f"{day_label} summary | fraud_sent={fraud_count} | normal_sent={normal_count}")


def main():
    start_time = time.time()
    base_date = datetime.strptime(START_DATE, "%Y-%m-%d")

    daily_fraud_ratios = get_daily_fraud_ratios()

    print("=" * 70)
    print("Streaming configuration")
    print(f"START_DATE: {START_DATE}")
    print(f"NUM_DAYS: {NUM_DAYS}")
    print(f"RECORDS_PER_DAY: {RECORDS_PER_DAY}")
    print(f"USE_AUTO_FRAUD_RATIOS: {USE_AUTO_FRAUD_RATIOS}")
    print(f"Daily fraud ratios: {daily_fraud_ratios}")
    print("=" * 70)

    fraud_pool, normal_pool = collect_balanced_pools_from_s3()

    total_all_days = 0
    total_fraud_all_days = 0
    total_normal_all_days = 0

    for day_index in range(NUM_DAYS):
        current_day = base_date + timedelta(days=day_index)
        day_label = current_day.strftime("%Y-%m-%d")
        fraud_ratio = daily_fraud_ratios[day_index]

        print("\n" + "=" * 70)
        print(f"Generating data for {day_label} | records={RECORDS_PER_DAY} | fraud_ratio={fraud_ratio:.2%}")
        print("=" * 70)

        demo_records = build_demo_records(
            fraud_pool=fraud_pool,
            normal_pool=normal_pool,
            total_records=RECORDS_PER_DAY,
            fraud_ratio=fraud_ratio
        )

        expected_fraud = int(RECORDS_PER_DAY * fraud_ratio)
        expected_normal = RECORDS_PER_DAY - expected_fraud

        send_records_to_kinesis_for_day(
            records=demo_records,
            day_start=current_day,
            day_label=day_label
        )

        total_all_days += RECORDS_PER_DAY
        total_fraud_all_days += expected_fraud
        total_normal_all_days += expected_normal

    elapsed = time.time() - start_time

    print("\n" + "#" * 70)
    print("ALL DAYS STREAMING COMPLETED")
    print(f"Date range: {START_DATE} to {(base_date + timedelta(days=NUM_DAYS - 1)).strftime('%Y-%m-%d')}")
    print(f"Total records sent: {total_all_days}")
    print(f"Expected fraud total: {total_fraud_all_days}")
    print(f"Expected normal total: {total_normal_all_days}")
    print(f"Total elapsed time: {elapsed:.2f} seconds")
    print("#" * 70)


if __name__ == "__main__":
    main()