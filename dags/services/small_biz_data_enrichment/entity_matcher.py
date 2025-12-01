import pandas as pd
import boto3
import io
import os
from difflib import SequenceMatcher


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localstack:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )


def similarity(a, b):
    """Calculates string similarity ratio (0.0 to 1.0)"""
    if pd.isna(a) or pd.isna(b): return 0.0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()


def match_and_merge_data():
    s3 = get_s3_client()
    obj_ct = s3.get_object(Bucket="staging-ct-data", Key="clean/cleaned.parquet")
    df_ct = pd.read_parquet(io.BytesIO(obj_ct['Body'].read()))

    obj_enrich = s3.get_object(Bucket="crux-data-enrichment", Key="normalized/biz_data.parquet")
    df_enrich = pd.read_parquet(io.BytesIO(obj_enrich['Body'].read()))

    unique_enrich_entities = df_enrich[['entity_id', 'name', 'state']].drop_duplicates()

    matches = []

    for _, enrich_row in unique_enrich_entities.iterrows():
        best_score = 0
        best_ct_id = None

        for _, ct_row in df_ct.iterrows():
            score = similarity(enrich_row['name'], ct_row['name'])

            if enrich_row['state'] and ct_row.get('mailing_address') and enrich_row['state'] in str(
                    ct_row['mailing_address']):
                score += 0.1

            if score > best_score:
                best_score = score
                best_ct_id = ct_row['id']

        if best_score > 0.8:
            matches.append({
                'enrichment_entity_id': enrich_row['entity_id'],
                'ct_business_id': best_ct_id,
                'match_score': best_score
            })

    df_matches = pd.DataFrame(matches)

    if df_matches.empty:
        df_final = pd.DataFrame(columns=['ct_business_id', 'official_business_name', 'mailing_address', 'week_offset',
                                         'business_volume_score', 'match_score'])
    else:
        df_merged = pd.merge(df_enrich, df_matches, left_on='entity_id', right_on='enrichment_entity_id', how='inner')

        df_final = pd.merge(df_merged, df_ct, left_on='ct_business_id', right_on='id', how='left')

        if 'name_y' in df_final.columns:
            df_final = df_final.rename(columns={'name_y': 'official_business_name'})
        else:
            df_final['official_business_name'] = df_final['name']

        target_cols = [
            'ct_business_id',
            'official_business_name',
            'mailing_address',
            'week_offset',
            'business_volume_score',
            'match_score'
        ]
        available_cols = [c for c in target_cols if c in df_final.columns]
        df_final = df_final[available_cols]

    print(f"Final Dataframe Shape: {df_final.shape}")
    out_buffer = io.BytesIO()
    df_final.to_parquet(out_buffer, index=False)

    s3.put_object(
        Bucket="staging-ct-data",
        Key="integrated/final_data.parquet",
        Body=out_buffer.getvalue()
    )
    print("Successfully saved Integrated Data to s3://staging-ct-data/integrated/final_data.parquet")