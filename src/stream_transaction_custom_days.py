import json
import time
import random
import uuid
import boto3
import pandas as pd
import argparse
from io import TextIOWrapper
from datetime import datetime, timedelta

# =========================
# Default configuration
# =========================
REGION = "us-east-1"
BUCKET_NAME = "finalproject-fraud-detection"
CSV_S3_KEY = "raw/PS_20174392719_1491204439457_log.csv"
STREAM_NAME = "fraud-stream"

CHUNK_SIZE = 200000
MAX_CANDIDATES = None
MAX_FRAUD_POOL = 30000
MAX_NORMAL_POOL = 120000

SHUFFLE_RECORDS = True
SEND_DELAY_SECONDS = 0.2
KINESIS_BATCH_SIZE = 200
PRINT_EVERY_BATCHES = 20

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

s3 = boto3.client("s3", region_name=REGION)
kinesis = boto3.client("kinesis", region_name=REGION)

locations = list(LOCATION_WEIGHTS.keys())
weights = list(LOCATION_WEIGHTS.values())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream demo transactions to Kinesis with configurable date range and fraud ratios."
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-03-17",
        help="Start date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--num-days",
        type=int,
        default=7,
        help="Number of days to generate data for."
    )
    parser.add_argument(
        "--records-per-day",
        type=int,
        default=100000,
        help="Number of records to send per day."
    )

    parser.add_argument(
        "--use-auto-fraud-ratios",
        action="store_true",
        help="Enable automatic fraud ratio generation for each day."
    )
    parser.add_argument(
        "--daily-fraud-ratios",
        type=str,
        default="0.05,0.08,0.12,0.18,0.10,0.22,0.15",
        help="Comma-separated daily fraud ratios, used when auto mode is disabled."
    )
    parser.add_argument(
        "--auto-fraud-min",
        type=float,
        default=0.05,
        help="Minimum fraud ratio for auto-generated daily ratios."
    )
    parser.add_argument(
        "--auto-fraud-max",
        type=float,
        default=0.25,
        help="Maximum fraud ratio for auto-generated daily ratios."
    )
    parser.add_argument(
        "--fraud-ratio-random-seed",
        type=int,
        default=42,
        help="Random seed for reproducible auto-generated fraud ratios."
    )

    parser.add_argument(
        "--send-delay-seconds",
        type=float,
        default=0.2,
        help="Delay between Kinesis batches."
    )
    parser.add_argument(
        "--kinesis-batch-size",
        type=int,
        default=200,
        help="Number of records per Kinesis put_records batch."
    )

    return parser.parse_args()


def parse_daily_fraud_ratios(ratio_text):
    ratios = []
    for item in ratio_text.split(","):
        item = item.strip()
        if item:
            ratios.append(float(item))
    return ratios


def validate_args(args):
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("start-date must be in YYYY-MM-DD format.") from exc

    if args.num_days <= 0:
        raise ValueError("num-days must be greater than 0.")

    if args.records_per_day <= 0:
        raise ValueError("records-per-day must be greater than 0.")

    if args.kinesis_batch_size <= 0 or args.kinesis_batch_size > 500:
        raise ValueError("kinesis-batch-size must be between 1 and 500.")

    if args.auto_fraud_min < 0 or args.auto_fraud_max > 1:
        raise ValueError("auto-fraud-min and auto-fraud-max must be between 0 and 1.")

    if args.auto_fraud_min > args.auto_fraud_max:
        raise ValueError("auto-fraud-min must be less than or equal to auto-fraud-max.")

    if not args.use_auto_fraud_ratios:
        manual_ratios = parse_daily_fraud_ratios(args.daily_fraud_ratios)
        if len(manual_ratios) < args.num_days:
            raise ValueError("The number of daily-fraud-ratios must be at least num-days.")
        for ratio in manual_ratios:
            if ratio < 0 or ratio > 1:
                raise ValueError("Each manual fraud ratio must be between 0 and 1.")


def random_location():
    return random.choices(locations, weights=weights, k=1)[0]


def random_event_time_for_day(day_start: datetime):
    """
    Generate a random timestamp within the specified day.
    """
    offset_seconds = random.randint(0, 86399)
    return (day_start + timedelta(seconds=offset_seconds)).isoformat() + "Z"


def get_daily_fraud_ratios(args):
    """
    Return fraud ratios for each day.

    Modes:
    1. Auto generation
    2. Manual ratios (free input)
    """

    if args.use_auto_fraud_ratios:

        if args.fraud_ratio_random_seed is not None:
            random.seed(args.fraud_ratio_random_seed)

        ratios = [
            round(random.uniform(args.auto_fraud_min, args.auto_fraud_max), 2)
            for _ in range(args.num_days)
        ]

        return ratios

    # manual free input mode
    manual_ratios = parse_daily_fraud_ratios(args.daily_fraud_ratios)

    if len(manual_ratios) == 0:
        raise ValueError("daily-fraud-ratios cannot be empty")

    result = []

    for i in range(args.num_days):
        result.append(manual_ratios[i % len(manual_ratios)])

    return result


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


def send_records_to_kinesis_for_day(records, day_start, day_label, send_delay_seconds, kinesis_batch_size):
    """
    Send one day's records to the Kinesis stream.
    """
    print(f"Ready to send {len(records)} demo transactions for {day_label} to Kinesis stream: {STREAM_NAME}")

    fraud_count = 0
    normal_count = 0
    sent_count = 0
    batch_count = 0

    indexed_records = list(enumerate(records, start=1))

    for batch in chunked(indexed_records, kinesis_batch_size):
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

            for entry, result in zip(entries, response.get("Records", [])):
                if "ErrorCode" in result:
                    retry_entries.append(entry)

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

        if send_delay_seconds > 0:
            time.sleep(send_delay_seconds)

    print(f"{day_label} streaming completed.")
    print(f"{day_label} summary | fraud_sent={fraud_count} | normal_sent={normal_count}")


def main():
    args = parse_args()
    validate_args(args)

    start_time = time.time()
    base_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    daily_fraud_ratios = get_daily_fraud_ratios(args)

    print("=" * 70)
    print("Streaming configuration")
    print(f"START_DATE: {args.start_date}")
    print(f"NUM_DAYS: {args.num_days}")
    print(f"RECORDS_PER_DAY: {args.records_per_day}")
    print(f"USE_AUTO_FRAUD_RATIOS: {args.use_auto_fraud_ratios}")
    print(f"DAILY_FRAUD_RATIOS: {daily_fraud_ratios}")
    print(f"SEND_DELAY_SECONDS: {args.send_delay_seconds}")
    print(f"KINESIS_BATCH_SIZE: {args.kinesis_batch_size}")
    print("=" * 70)

    fraud_pool, normal_pool = collect_balanced_pools_from_s3()

    total_all_days = 0
    total_fraud_all_days = 0
    total_normal_all_days = 0

    for day_index in range(args.num_days):
        current_day = base_date + timedelta(days=day_index)
        day_label = current_day.strftime("%Y-%m-%d")
        fraud_ratio = daily_fraud_ratios[day_index]

        print("\n" + "=" * 70)
        print(
            f"Generating data for {day_label} | "
            f"records={args.records_per_day} | "
            f"fraud_ratio={fraud_ratio:.2%}"
        )
        print("=" * 70)

        demo_records = build_demo_records(
            fraud_pool=fraud_pool,
            normal_pool=normal_pool,
            total_records=args.records_per_day,
            fraud_ratio=fraud_ratio
        )

        expected_fraud = int(args.records_per_day * fraud_ratio)
        expected_normal = args.records_per_day - expected_fraud

        send_records_to_kinesis_for_day(
            records=demo_records,
            day_start=current_day,
            day_label=day_label,
            send_delay_seconds=args.send_delay_seconds,
            kinesis_batch_size=args.kinesis_batch_size
        )

        total_all_days += args.records_per_day
        total_fraud_all_days += expected_fraud
        total_normal_all_days += expected_normal

    elapsed = time.time() - start_time

    print("\n" + "#" * 70)
    print("ALL DAYS STREAMING COMPLETED")
    print(
        f"Date range: {args.start_date} to "
        f"{(base_date + timedelta(days=args.num_days - 1)).strftime('%Y-%m-%d')}"
    )
    print(f"Total records sent: {total_all_days}")
    print(f"Expected fraud total: {total_fraud_all_days}")
    print(f"Expected normal total: {total_normal_all_days}")
    print(f"Total elapsed time: {elapsed:.2f} seconds")
    print("#" * 70)


if __name__ == "__main__":
    main()