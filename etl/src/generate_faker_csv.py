import os
import boto3
from botocore.client import Config

from faker import Faker
import random
import csv

START_DATE = "2026-05-05"
END_DATE = "2026-05-14"
CSV_PATH = "faker_data.csv"

Faker.seed(42)
random.seed(42)
faker = Faker()


def upload_to_minio(local_path, minio_key, bucket="stage"):
    minio_endpoint = "http://localhost:9002"
    minio_access_key = "minio"
    minio_secret_key = "minio123"
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    s3_client.upload_file(local_path, bucket, minio_key)
    print(f"Uploaded {local_path} to s3://{bucket}/{minio_key}")
    os.remove(local_path)

def generate_random_faker_data_per_day(index,start_date, end_date, base_csv_path):
    from datetime import datetime, timedelta
    import time
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta_days = (end_dt - start_dt).days + 1
    fake_id = 0
    for day in range(delta_days):
        fake_date = start_dt + timedelta(days=day)
        # Random number of records for this day
        num_records = random.randint(1, 50)
        # Generate a random time for the file epoch (for filename uniqueness)
        rand_hour = random.randint(0, 23)
        rand_min = random.randint(0, 59)
        rand_sec = random.randint(0, 59)
        fake_datetime = fake_date.replace(hour=rand_hour, minute=rand_min, second=rand_sec)
        epoch = int(time.mktime(fake_datetime.timetuple()))
        timestamp_str = fake_datetime.strftime('%Y-%m-%dT%H-%M-%S')
        csv_path = f"{index}/{base_csv_path.rstrip('.csv')}_{timestamp_str}_{epoch}.csv"
        import os
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        with open(csv_path, mode="w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["id", "event_ts", "payload"])
            for _ in range(num_records):
                fake_time = faker.time(pattern="%H:%M:%S")
                event_ts = f"{fake_date.strftime('%Y-%m-%d')} {fake_time}"
                fake_payload = faker.text(max_nb_chars=40)
                writer.writerow([fake_id, event_ts, fake_payload])
                fake_id += 1
        print(f"Wrote {num_records} rows to {csv_path}")

        # Upload to MinIO
        minio_key = f"{index}/{os.path.basename(csv_path)}"
        upload_to_minio(csv_path, minio_key)

_UPI_EVENT_TYPES = [
    "payment_initiated", "payment_success", "payment_failed",
    "money_received", "balance_check", "upi_pin_change",
    "account_linked", "login", "logout", "otp_verified",
    "autopay_setup", "collect_request_sent", "refund_initiated", "refund_success",
]
_UPI_BANKS = ["SBI", "HDFC", "ICICI", "Axis", "Kotak", "PNB", "BOB", "Canara", "Union Bank", "IDFC First"]
_UPI_HANDLES = ["@okicici", "@okhdfcbank", "@oksbi", "@okaxis", "@ybl", "@paytm"]
_PAYMENT_EVENTS = frozenset({"payment_initiated", "payment_success", "money_received", "refund_success"})


def generate_upi_events(
    total_records=1_000_000,
    batch_size=500_000,
    bucket="stage",
    prefix="upi_events",
):
    """
    Generate synthetic UPI mobile-app events and stream them to MinIO in batches.
    IDs are monotonically increasing from 0. Timestamps span 2025-05-01 to 2026-05-25.
    Requires numpy and pandas.
    """
    from datetime import datetime, timezone
    import math
    import numpy as np
    import pandas as pd

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rng = np.random.default_rng(42)
    events_arr = np.array(_UPI_EVENT_TYPES)
    banks_arr  = np.array(_UPI_BANKS)
    statuses   = np.array(["success", "failed", "pending"])

    start_ts = int(datetime(2025, 5, 1).timestamp())
    end_ts   = int(datetime(2026, 5, 25, 23, 59, 59).timestamp())
    ts_range = end_ts - start_ts

    # Build user pool once using Faker, then sample with numpy
    pool_size = 50_000
    print(f"Building {pool_size:,} synthetic UPI users ...")
    first_names = [faker.first_name().lower().replace(" ", "") for _ in range(pool_size)]
    last_names  = [faker.last_name().lower().replace(" ", "") for _ in range(pool_size)]
    handles     = rng.choice(_UPI_HANDLES, pool_size)
    upi_ids     = np.array([f"{fn}.{ln}{h}" for fn, ln, h in zip(first_names, last_names, handles)])
    # Indian mobile numbers: leading digit 6–9, then 9 more digits
    user_ids    = np.char.add(
        rng.integers(6, 10, pool_size).astype(str),
        rng.integers(100_000_000, 999_999_999, pool_size).astype(str),
    )
    device_pool = np.array([faker.uuid4() for _ in range(10_000)])

    num_batches = math.ceil(total_records / batch_size)
    print(f"Generating {total_records:,} events in {num_batches:,} batches → s3://{bucket}/{prefix}/")

    for batch_idx in range(num_batches):
        id_start = batch_idx * batch_size
        n        = min(batch_size, total_records - id_start)

        ts_raw   = start_ts + rng.integers(0, ts_range, n)
        ev_idx   = rng.integers(0, len(events_arr), n)
        usr_idx  = rng.integers(0, pool_size, n)
        bnk_idx  = rng.integers(0, len(banks_arr), n)
        dev_idx  = rng.integers(0, len(device_pool), n)
        stat_idx = rng.choice(3, n, p=[0.85, 0.12, 0.03])
        is_pay   = np.isin(events_arr[ev_idx], list(_PAYMENT_EVENTS))
        amounts  = np.where(
            is_pay,
            np.round(rng.exponential(500, n).clip(1, 200_000), 2),
            np.nan,
        )

        df = pd.DataFrame({
            "id":         id_start + np.arange(n, dtype=np.int64),
            "event_ts":   pd.to_datetime(ts_raw, unit="s").astype("datetime64[us]"),
            "user_id":    user_ids[usr_idx],
            "upi_id":     upi_ids[usr_idx],
            "event_type": events_arr[ev_idx],
            "bank":       banks_arr[bnk_idx],
            "amount":     amounts,
            "status":     statuses[stat_idx],
            "device_id":  device_pool[dev_idx],
        })

        pq_path   = f"/tmp/upi_{run_id}_batch_{batch_idx:07d}.parquet"
        minio_key = f"{prefix}/{run_id}/batch_{batch_idx:07d}.parquet"
        df.to_parquet(pq_path, index=False, engine="pyarrow", compression="snappy")
        upload_to_minio(pq_path, minio_key, bucket=bucket)

        done = id_start + n
        if (batch_idx + 1) % 50 == 0 or done == total_records:
            print(f"  {done:>15,} / {total_records:,}  ({done / total_records * 100:.1f}%)")

    print(f"Done. {total_records:,} UPI events written to s3://{bucket}/{prefix}/")


if __name__ == "__main__":
    # Generate one file per day with a random number of records
    for i in range(10):
        generate_random_faker_data_per_day(i,START_DATE, END_DATE, CSV_PATH)
