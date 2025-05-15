"""Create dag to simulate social media pipeline."""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator

import boto3
import time

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BUCKET = "fcorp-data-prod"
RAW_PREFIX = "raw/marketing/social_media/src=meta/"
CONSUMPTION_PREFIX = "staging/marketing/social_media/src=meta/"
REGION = "us-east-1"
ATHENA_DB = "marketing_db"
ATHENA_TABLE = "meta_daily_campaigns"
OUTPUT_LOCATION = f"s3://{BUCKET}/athena_query_results/"


def get_glue_client():
    """Get Glue client."""
    session = boto3.Session(region_name=REGION)
    return session.client("glue")


def execute_glue_job(job_name, glue_client, script_args=None):
    """Execute Glue job."""
    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments=script_args or {}
    )
    return response["JobRunId"]


def wait_for_glue_job_completion(job_name, job_run_id, glue_client):
    """Wait for Glue job completion."""
    while True:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response["JobRun"]["JobRunState"]
        if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
            return status
        time.sleep(30)


def execute_glue_job_and_wait(job_name, script_args=None):
    """Execute Glue job and wait for completion."""
    glue_client = get_glue_client()
    job_run_id = execute_glue_job(job_name, glue_client)
    status = wait_for_glue_job_completion(job_name, job_run_id, glue_client)
    if status != "SUCCEEDED":
        raise Exception(f"Glue job {job_name} failed with status: {status}")


with DAG(
    dag_id="af_glue_meta_marketing_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Simula, transforma y valida datos de marketing usando Glue + S3 + Athena",
    tags=["glue", "athena", "s3"],
) as dag:

    # 1. Simular datos de marketing
    simulate_data = PythonOperator(
        task_id="simulate_marketing_data",
        python_callable=execute_glue_job_and_wait,
        op_args=["sdata-ingestion-social-media"],
        provide_context=True,
    )

    # 2. Validar archivo generado (por ejemplo daily_data.csv)
    wait_for_csv = S3KeySensor(
        task_id="wait_for_csv",
        bucket_name=BUCKET,
        bucket_key=f"{RAW_PREFIX}campaigns.csv",  # ajusta si va a otra ruta
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=300,
    )

    # 3. Transformar datos simulados
    transform_data = PythonOperator(
        task_id="transform_social_media_data",
        python_callable=execute_glue_job_and_wait,
        op_args=["data-transformation-social-media"],
        provide_context=True,
    )

    # 4. Crear/Actualizar tabla en Athena
    create_athena_table = AthenaOperator(
        task_id="create_athena_table",
        query=f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE} (
            campaign_id string,
            ad_group_id string,
            ad_group_name string,
            ad_id string,
            ad_name string,
            platform string,
            date string,
            marca string,
            tipo string,
            audience string,
            ad_type string,
            impressions int,
            clicks int,
            spend double,
            interactions int,
            conversions int,
            quartile_25 int,
            quartile_50 int,
            quartile_75 int,
            completed int
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
            'serialization.format' = ',',
            'field.delim' = ','
        )
        LOCATION 's3://{BUCKET}/{CONSUMPTION_PREFIX}'
        TBLPROPERTIES ('has_encrypted_data'='false');
        ''',
        database=ATHENA_DB,
        output_location=OUTPUT_LOCATION,
        aws_conn_id="aws_default",
    )

    # Orquestación
    simulate_data >> wait_for_csv >> transform_data >> create_athena_table
