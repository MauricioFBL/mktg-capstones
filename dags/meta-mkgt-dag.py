"""Create dag to simulate social media pipeline."""

import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    "owner": "mbautista",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

BUCKET = "fcorp-data-prod"
RAW_PREFIX = "raw/marketing/social_media/src=meta/"
STG_PREFIX = "staging/marketing/social_media/src=meta/"
CON_PREFIX = "consumption/marketing/social_media/meta_monthly/"
REGION = "us-east-1"
ATHENA_DB = "marketing_db"
ATHENA_TABLE_STG = "meta_daily_campaigns"
ATHENA_TABLE_CON = "meta_monthly_summary"
OUTPUT_LOCATION = f"s3://{BUCKET}/athena_query_results/"


def get_glue_client() -> boto3.client:
    """Get Glue client.

    Create and return a boto3 Glue client configured for the specified AWS region.

    Returns:
        boto3.client: A Glue client instance for interacting with AWS Glue.

    """
    session = boto3.Session(region_name=REGION)
    return session.client("glue")


def execute_glue_job(job_name: str, glue_client: boto3.client, script_args: dict = None) -> str:
    """Execute Glue job.

    Start an AWS Glue job with optional script arguments.

    Args:
        job_name (str): The name of the Glue job to execute.
        glue_client (boto3.client): A boto3 Glue client.
        script_args (dict, optional): Dictionary of arguments to pass to the Glue job. Defaults to None.

    Returns:
        str: The JobRunId of the started Glue job.

    """
    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments=script_args or {}
    )
    return response["JobRunId"]


def wait_for_glue_job_completion(job_name: str, job_run_id: str, glue_client: boto3.client) -> str:
    """Wait for Glue job completion.

    Wait for an AWS Glue job to finish execution.

    This function polls the Glue job status every 60 seconds until the job completes
    (either SUCCEEDED, FAILED, or STOPPED).

    Args:
        job_name (str): The name of the Glue job.
        job_run_id (str): The ID of the Glue job run.
        glue_client (boto3.client): A boto3 Glue client.

    Returns:
        str: The final status of the Glue job run.

    """
    status = "RUNNING"
    while status not in ["SUCCEEDED", "FAILED", "STOPPED"]:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response["JobRun"]["JobRunState"]
        time.sleep(60)

    return status


def execute_glue_job_and_wait(job_name: str, script_args: dict = None):
    """Execute Glue job and wait for completion.

    Execute an AWS Glue job and wait for its completion.

    Raises an exception if the job fails or is stopped.

    Args:
        job_name (str): The name of the Glue job to run.
        script_args (dict, optional): Dictionary of arguments to pass to the Glue job. Defaults to None.

    Raises:
        Exception: If the Glue job does not complete successfully.

    """
    glue_client = get_glue_client()
    job_run_id = execute_glue_job(job_name, glue_client)
    status = wait_for_glue_job_completion(job_name, job_run_id, glue_client)
    if status != "SUCCEEDED":
        raise Exception(f"Glue job {job_name} failed with status: {status}")
    glue_client.close()


with DAG(
    dag_id="af_glue_meta_marketing_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Simula, transforma y valida datos de marketing usando Glue + S3 + Athena",
    tags=["glue", "athena", "s3"],
    doc_md="""
    ### DAG: af_glue_meta_marketing_pipeline

    Este DAG automatiza un flujo de trabajo de marketing digital que incluye:

    1. **Simulación de datos** en S3 (raw).
    2. **Validación del archivo generado** mediante sensor en S3.
    3. **Transformación** y agregación mensual con AWS Glue.
    4. **Creación/actualización** de tablas en Athena:
        - Tabla de staging (`meta_daily_campaigns`)
        - Tabla de consumo (`meta_monthly_summary`)
    5. **Repair table** para registrar particiones en la tabla de consumo.

    **Tecnologías**: AWS Glue, S3, Athena, Airflow

    **Autor**: mbautista
    """
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
    create_athena_db = AthenaOperator(
        task_id="create_athena_db",
        query="CREATE DATABASE IF NOT EXISTS marketing_db;",
        database="default",  # O cualquier DB existente
        output_location=OUTPUT_LOCATION,
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    # 5. Crear/Actualizar tabla en Athena
    create_athena_table = AthenaOperator(
        task_id="create_athena_table",
        query=f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE_STG} (
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
        LOCATION 's3://{BUCKET}/{STG_PREFIX}'
        TBLPROPERTIES (
            'has_encrypted_data'='false',
            'skip.header.line.count'='1'
        );
        ''',
        database=ATHENA_DB,
        output_location=OUTPUT_LOCATION,
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    # 6. Transformar datos simulados
    create_meta_data_summary = PythonOperator(
        task_id="create_meta_data_summary",
        python_callable=execute_glue_job_and_wait,
        op_args=["data-consumption-social-media-meta"],
        provide_context=True,
    )

    # 7. Crear/Actualizar tabla en Athena consumption
    create_athena_consumption_table = AthenaOperator(
        task_id="create_athena_consumption_table",
        query=f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE_CON} (
            campaign_id           string,
            ad_group_name         string,
            ad_name               string,
            platform              string,
            marca                 string,
            tipo                  string,
            audience              string,
            ad_type               string,
            impressions           bigint,
            clicks                bigint,
            spend                 double,
            conversions           bigint,
            completed             bigint,
            CTR                   double,
            CPC                   double,
            CPA                   double,
            ROI                   double,
            CPM                   double,
            completion_rate       double,
            CTR_3m                double,
            CPA_3m                double,
            ROI_3m                double,
            spend_3m              double,
            conversions_3m        bigint,
            last_updated_date     date
        )
        PARTITIONED BY (
            year                  int,
            month                 int
            )
        STORED AS PARQUET
        LOCATION 's3://{BUCKET}/{CON_PREFIX}'
        TBLPROPERTIES (
            'parquet.compress' = 'SNAPPY',
            'has_encrypted_data' = 'false'
        );
        ''',
        database=ATHENA_DB,
        output_location=OUTPUT_LOCATION,
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    # 8. Crear/Actualizar tabla en Athena consumption
    repair_athena_table = AthenaOperator(
        task_id="repair_athena_table_consumption",
        query=f"MSCK REPAIR TABLE {ATHENA_DB}.{ATHENA_TABLE_CON};",
        database=ATHENA_DB,
        output_location=OUTPUT_LOCATION,
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    # Orquestación
    simulate_data >> wait_for_csv >> transform_data >> create_athena_db >> create_athena_table >> create_meta_data_summary >> create_athena_consumption_table >> repair_athena_table
