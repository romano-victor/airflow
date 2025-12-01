from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from services.small_biz_data_enrichment.ct_data_handler import extract_ct_data, quality_check_ct_data
from services.small_biz_data_enrichment.business_data_handler import normalize_business_data
from services.small_biz_data_enrichment.entity_matcher import match_and_merge_data

with DAG(
    dag_id="ct_business_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
):
    normalize = PythonOperator(
        task_id="normalize_business_data",
        python_callable=normalize_business_data,
    )

    extract = PythonOperator(
        task_id="extract_socrata",
        python_callable=extract_ct_data,
        op_kwargs={"dataset_id": "n7gp-d28j"},
    )

    quality_check = PythonOperator(
        task_id="quality_check_ct_data",
        python_callable=quality_check_ct_data,
    )

    match_and_merge_data = PythonOperator(
        task_id="match_and_merge_data",
        python_callable=match_and_merge_data
    )

    extract >> quality_check
    [normalize, quality_check] >> match_and_merge_data
