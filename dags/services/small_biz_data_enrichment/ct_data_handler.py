import pandas as pd
import boto3
import io
import os
import logging
import numpy as np

from sodapy import Socrata
from time import sleep
from pydantic import ValidationError
from database.schemas import BusinessSchema

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

endpoint = os.getenv("S3_ENDPOINT", "http://localstack:4566")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)


def extract_ct_data(dataset_id: str, chunk_size: int = 50000, token=None):
    client = Socrata("data.ct.gov", token)
    offset = 0
    chunk_index = 0

    while True:
        sleep(3)
        rows = client.get(dataset_id, limit=chunk_size, offset=offset)

        if not rows:
            break

        df = pd.DataFrame.from_records(rows)

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket="raw-ct-data",
            Key=f"chunks/chunk_{chunk_index}.parquet",
            Body=buffer.getvalue()
        )
        logger.info(f"Uploaded chunk {chunk_index}")

        if len(df) < chunk_size:
            break

        chunk_index += 1
        offset += chunk_size


def quality_check_ct_data():
    logger.info("Starting Data Quality Checks with Pydantic...")

    result = s3.list_objects_v2(Bucket="raw-ct-data", Prefix="chunks/")
    if "Contents" not in result:
        raise ValueError("DQ Failed: No data found in raw-ct-data bucket")

    keys = [o["Key"] for o in result["Contents"]]
    chunks = []

    for key in keys:
        obj = s3.get_object(Bucket="raw-ct-data", Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        chunks.append(df)

    if not chunks:
        raise ValueError("DQ Failed: No parquet chunks loaded")

    final_df = pd.concat(chunks, ignore_index=True)

    if final_df.empty:
        raise ValueError("DQ Failed: Dataset is empty")

    final_df.drop_duplicates(inplace=True)

    final_df = final_df.replace({np.nan: None})

    records = final_df.to_dict(orient="records")
    valid_records = []
    error_count = 0

    for record in records:
        try:
            validated_record = BusinessSchema(**record)
            valid_records.append(validated_record.model_dump())
        except ValidationError:
            error_count += 1
            continue

    if error_count > 0:
        logger.warning(f"DQ Warning: Dropped {error_count} rows due to validation errors.")

    if not valid_records:
        raise ValueError("DQ Failed: All rows failed validation")

    validated_df = pd.DataFrame(valid_records)

    buffer = io.BytesIO()
    validated_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket="staging-ct-data",
        Key="clean/cleaned.parquet",
        Body=buffer.getvalue()
    )
    logger.info("Data Quality Checks Passed. Moving data to staging.")