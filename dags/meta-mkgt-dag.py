"""Create dag to simulate social media pipeline."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BUCKET = "fcorp-data-prod"
RAW_PREFIX = "raw/"
CONSUMPTION_PREFIX = "consumption/social_media/"
REGION = "us-east-1"
ATHENA_DB = "marketing_db"
ATHENA_TABLE = "meta_daily_campaigns"
OUTPUT_LOCATION = f"s3://{BUCKET}/athena_query_results/"

with DAG(
    dag_id="glue_marketing_pipeline_with_validation",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Simula, transforma y valida datos de marketing usando Glue + S3 + Athena",
    tags=["glue", "athena", "s3"],
) as dag:

    # 1. Simular datos en S3
    simulate_data = GlueJobOperator(
        task_id="simulate_marketing_data",
        job_name="simulate-marketing-data",
        aws_conn_id="aws_default",
        region_name=REGION,
    )

    # 2. Validar archivo generado (por ejemplo daily_data.csv)
    wait_for_csv = S3KeySensor(
        task_id="wait_for_csv",
        bucket_name=BUCKET,
        bucket_key=f"{RAW_PREFIX}daily_data.csv",  # ajusta si va a otra ruta
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=300,
    )

    # 3. Transformar datos simulados
    transform_data = GlueJobOperator(
        task_id="transform_social_media_data",
        job_name="data-transformation-social-media",
        aws_conn_id="aws_default",
        region_name=REGION,
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
