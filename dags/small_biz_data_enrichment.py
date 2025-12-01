from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from services.small_biz_data_enrichment.ct_data_handler import extract_ct_data, quality_check_ct_data, load_ct_data

with DAG(
    dag_id="ct_business_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
):

    extract = PythonOperator(
        task_id="extract_socrata",
        python_callable=extract_ct_data,
        op_kwargs={"dataset_id": "n7gp-d28j"},
    )

    quality_check = PythonOperator(
        task_id="quality_check_ct_data",
        python_callable=quality_check_ct_data,
    )

    load = PythonOperator(
        task_id="load_ct_data",
        python_callable=load_ct_data,
    )

    extract >> quality_check >> load