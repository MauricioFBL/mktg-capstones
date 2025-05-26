"""Create dag to simulate social media pipeline."""

import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

default_args = {
    "owner": "eloranca",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

BUCKET = "fcorp-data-prod"
ORIG_FILE_PATH = "raw/policies/orig_file/"
RAW_PATH = "raw/policies/tables"
CON_PATH = "consumption/policies/cons_policies/"
REGION = "us-east-1"
ATHENA_DB = "marketing_db"
ATHENA_TABLE = "policies_final_table"
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
        time.sleep(20)


def execute_glue_job_and_wait(job_name, script_args=None):
    """Execute Glue job and wait for completion."""
    glue_client = get_glue_client()
    job_run_id = execute_glue_job(job_name, glue_client)
    status = wait_for_glue_job_completion(job_name, job_run_id, glue_client)
    if status != "SUCCEEDED":
        raise Exception(f"Glue job {job_name} failed with status: {status}")

with DAG(
    dag_id = "el_glue_policies_pipeline",
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    description = "Simulacion de un pipeline que procesa datos relacionados a polizas de automoviles usando Athena, S3 y Glue.",
    tags = ["glue", "athena", "s3"],
    doc_md = """
    ### DAG: el_glue_policies_pipeline

    Este DAG automatiza un flujo de trabajo de polizas de automoviles que incluye:

    1. **Simulación de multiples archivos** en S3 (raw).
    2. **Consolidacion** de todos los archivos en s3 (staging).
    3. **Transformacion** y creacion de columnas utiles para analitica (consumption).
    4. **Creación/actualización** de tablas en Athena:
        - Tabla productiva (`policies_final_table`)
    5. **Repair table** para registrar particiones en la tabla de consumo.

    **Tecnologías**: AWS Glue, S3, Athena, Airflow

    **Autor**: airflow
    """
) as dag:

    # TAREA 1: Creacion de multiples archivos en zona raw para simulacion.
    create_mult_files = PythonOperator(
        task_id = "create_mult_files_raw_process",
        python_callable = execute_glue_job_and_wait,
        op_args = ["01-simulate-multiples-tables"],
        provide_context = True
    )

    # TAREA 2: Consolidacion de los archivos y escritura en zona staging.
    stg_process = PythonOperator(
        task_id = "consolidate_stg_process",
        python_callable = execute_glue_job_and_wait,
        op_args = ["02-data-clean-big-table"],
        provide_context = True
    )

    # TAREA 3: Transformaciones para analitica en zona consumption.
    cons_process = PythonOperator(
        task_id = "transformations_cons_process",
        python_callable = execute_glue_job_and_wait,
        op_args = ["03-policies-consumption"],
        provide_context = True
    )

    # TAREA 4: Creacion Base de Datos "marketing_db" en Athena.
    create_athena_db = AthenaOperator(
        task_id="create_athena_db",
        query="CREATE DATABASE IF NOT EXISTS marketing_db;",
        database="default",  # O cualquier DB existente
        output_location=OUTPUT_LOCATION,
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    # TAREA 5: Creacion/Actualizacion de tabla policies_final_table en Athena.
    create_athena_table = AthenaOperator(
        task_id = "create_policies_athena_table",
        query = f'''
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_TABLE}(
            Customer STRING,
            Response STRING,
            Coverage STRING,
            Education STRING,
            `Effective To Date` DATE,
            EmploymentStatus STRING,
            Gender STRING,
            `Location Code` STRING,
            `Marital Status` STRING,
            `Monthly Premium Auto` INT,
            `Months Since Last Claim` INT,
            `Months Since Policy Inception` INT,
            `Number of Open Complaints` INT,
            `Number of Policies` INT,
            `Renew Offer Type` STRING,
            `Sales Channel` STRING,
            `Total Claim Amount` DOUBLE,
            `Vehicle Class` STRING,
            `Vehicle Size` STRING,
            `Policy Type` STRING,
            Policy STRING,
            State STRING,
            customer_lifetime_value DOUBLE,
            income_by_month DOUBLE,
            premium_income_ratio DOUBLE,
            complaint_rate DOUBLE,
            clv_categorical STRING,
            clv_categorical_percent DOUBLE
        )
        PARTITIONED BY (
            load_date DATE
        )
        STORED AS PARQUET
        LOCATION 's3://{BUCKET}/{CON_PATH}'
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

    # TAREA 6: Ejecucion de un repair table para registrar particiones.
    repair_athena_table = AthenaOperator(
    task_id="repair_athena_table_consumption",
    query=f"MSCK REPAIR TABLE {ATHENA_DB}.{ATHENA_TABLE};",
    database=ATHENA_DB,
    output_location=OUTPUT_LOCATION,
    aws_conn_id="aws_default",
    region_name=REGION
    )

    # TAREA 7: Borrado de archivos en zona raw.
    delete_raw_files = S3DeleteObjectsOperator(
        task_id = 'delete_raw_files',
        bucket = BUCKET,
        keys = [f'{RAW_PATH}/policies_types.csv',
                f'{RAW_PATH}/policies_levels.csv',
                f'{RAW_PATH}/states.csv',
                f'{RAW_PATH}/transactions.csv'],
        aws_conn_id = 'aws_default'
    )

    # Orquestacion del flujo de tareas.
    create_mult_files >> stg_process >> cons_process >> create_athena_db >> create_athena_table >> repair_athena_table >> delete_raw_files
