import os
import boto3
import json
import pandas as pd
import io


def normalize_business_data():
    endpoint = os.getenv("S3_ENDPOINT", "http://localstack:4566")

    bucket_name = "crux-data-enrichment"
    file_key = "ingestion/small_biz_data.json"
    output_key = "normalized/biz_data.parquet"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    try:
        print(f"--- Reading file from s3://{bucket_name}/{file_key} ---")

        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()
        raw_data = json.loads(file_content)
        df = pd.DataFrame(raw_data)

        def extract_state(address):
            if not address:
                return None
            parts = address.split(',')
            if len(parts) >= 2:
                return parts[-2].strip()
            return None

        df['state'] = df['address'].apply(extract_state)

        normalized_records = []
        for _, row in df.iterrows():
            volumes = row.get('business_volume')
            if isinstance(volumes, dict):
                for week, score in volumes.items():
                    record = row.drop('business_volume').to_dict()
                    record['week_offset'] = int(week)
                    record['business_volume_score'] = score
                    normalized_records.append(record)
            else:
                record = row.drop('business_volume').to_dict()
                record['week_offset'] = None
                record['business_volume_score'] = None
                normalized_records.append(record)

        df_final = pd.DataFrame(normalized_records)
        cols = ['entity_id', 'name', 'state', 'week_offset', 'business_volume_score'] + \
               [c for c in df_final.columns if
                c not in ['entity_id', 'name', 'state', 'week_offset', 'business_volume_score']]
        df_final = df_final[cols]

        out_buffer = io.BytesIO()
        df_final.to_parquet(out_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=output_key, Body=out_buffer.getvalue())

    except Exception as e:
        print(f"CRITICAL ERROR in normalize_business_data: {e}")
        raise e