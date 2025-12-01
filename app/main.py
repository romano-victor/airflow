import os
import boto3
import sys

endpoint = os.getenv("S3_ENDPOINT", "http://localstack:4566")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

buckets = [
    "crux-data-enrichment",
    "staging-ct-data",
    "raw-ct-data"
]

print(f"--- Initializing Buckets at {endpoint} ---")
for bucket in buckets:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' already exists.")
    except:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"Created bucket '{bucket}'.")
        except Exception as e:
            print(f"Failed to create bucket '{bucket}': {e}")

bucket = "crux-data-enrichment"
key = "ingestion/small_biz_data.json"

possible_paths = [
    "data/small_biz_data.json",
    "app/data/small_biz_data.json",
    "small_biz_data.json",
    "../app/data/small_biz_data.json"
]

local_path = None
for path in possible_paths:
    if os.path.exists(path):
        local_path = path
        break

if local_path:
    print(f"--- Uploading Data ---")
    print(f"Found file at: {local_path}")
    try:
        s3.upload_file(local_path, bucket, key)
        print(f"✅ Successfully uploaded to s3://{bucket}/{key}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
else:
    print("❌ Error: Could not find 'small_biz_data.json'. Checked paths:")
    for p in possible_paths:
        print(f" - {p}")
    sys.exit(1)