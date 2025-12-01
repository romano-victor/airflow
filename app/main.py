import os
import boto3

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

for bucket in buckets:
    try:
        s3.head_bucket(Bucket=bucket)
    except:
        s3.create_bucket(Bucket=bucket)

bucket = "crux-data-enrichment"
key = "ingestion/small_biz_data.json"
local_path = "data/small_biz_data.json"