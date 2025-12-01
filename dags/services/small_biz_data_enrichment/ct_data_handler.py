import pandas as pd
import boto3
import io
import os
import logging
import numpy as np
import gc

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


def extract_ct_data(dataset_id: str, chunk_size: int = 10000, token=None):
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
    logger.info("Starting Data Quality Checks with Pydantic (Memory Optimized)...")

    result = s3.list_objects_v2(Bucket="raw-ct-data", Prefix="chunks/")
    if "Contents" not in result:
        raise ValueError("DQ Failed: No data found in raw-ct-data bucket")

    keys = [o["Key"] for o in result["Contents"]]

    valid_dfs = []  # Store only valid DataFrames, not raw dictionaries
    total_dropped = 0

    for key in keys:
        logger.info(f"Processing chunk: {key}")

        obj = s3.get_object(Bucket="raw-ct-data", Key=key)
        df_chunk = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        if df_chunk.empty:
            continue

        records = df_chunk.replace({np.nan: None}).to_dict(orient="records")

        chunk_valid_records = []

        for record in records:
            try:
                validated_record = BusinessSchema(**record)
                chunk_valid_records.append(validated_record.model_dump())
            except ValidationError:
                total_dropped += 1

        if chunk_valid_records:
            valid_dfs.append(pd.DataFrame(chunk_valid_records))

        del df_chunk
        del records
        del chunk_valid_records
        gc.collect()

    if not valid_dfs:
        raise ValueError("DQ Failed: All rows across all chunks failed validation")

    logger.warning(f"DQ Warning: Dropped {total_dropped} rows total due to validation errors.")

    logger.info("Concatenating valid chunks...")
    final_df = pd.concat(valid_dfs, ignore_index=True)

    del valid_dfs
    gc.collect()

    logger.info("Dropping duplicates...")
    final_df.drop_duplicates(inplace=True)

    logger.info("Writing cleaned data to S3...")
    buffer = io.BytesIO()
    final_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket="staging-ct-data",
        Key="clean/cleaned.parquet",
        Body=buffer.getvalue()
    )
    logger.info("Data Quality Checks Passed. Moving data to staging.")