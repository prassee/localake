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
    try:
        s3_client.upload_file(local_path, bucket, minio_key)
        print(f"Uploaded {local_path} to s3://{bucket}/{minio_key}")
        os.remove(local_path)
        print(f"Deleted local file {local_path}")
    except Exception as e:
        print(f"Failed to upload {local_path} to MinIO: {e}")

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

if __name__ == "__main__":
    # Generate one file per day with a random number of records
    for i in range(10):
        generate_random_faker_data_per_day(i,START_DATE, END_DATE, CSV_PATH)
