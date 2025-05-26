"""Glue job to get policies data from clean stage, applies transformations and write on consumption stage."""

import logging
import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, round, when
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_load_date() -> date:
    """Get today date.

    Returns:
        Date: Audit date.

    """
    today_date = date.today()

    logger.info(f"Date use to process: {today_date}")
    return today_date


def get_staging_s3_location() -> str:
    """Get the S3 location for storing staging CSV file.

    Returns:
        str: S3 location.

    """
    return 's3://fcorp-data-prod/staging/policies'


def get_consumption_s3_location() -> str:
    """Get the S3 location for storing consumption CSV file.

    Returns:
        str: S3 location.

    """
    return 's3://fcorp-data-prod/consumption/policies'


def read_data(spark: SparkSession) -> DataFrame:
    """Read staging data.

    Args:
        spark: SparkSession for spark.

    Returns:
        Dataframe: Dataframe from staging.

    """
    df = spark.read.parquet(f'{get_staging_s3_location()}/stg_policies')
    return df

# Bussiness Logic - Transformations
def filter_and_cast(df: DataFrame, partition_date: date) -> DataFrame:
    """Filter partition and cast columns to correct types.

    Args:
        df (Dataframe): Input dataframe.
        partition_date(date): Date used to filter dataframe.

    Returns:
        Dataframe with data to process and correct columns types.

    """
    df = df.filter((col('load_date') == lit(partition_date)) &
                   (col('EmploymentStatus') == lit('Employed')))
    df = df.select('*',
                   col('Income').cast('int').alias('income_by_year'),
                   round(col('Customer Lifetime Value'), 2).alias('customer_lifetime_value').cast('float')
                   ).drop('Income',
                           'Customer Lifetime Value',
                           'load_date')
    return df


def create_income_by_year_column(df: DataFrame) -> DataFrame:
    """Create income_by_year column.

    Args:
        df(Dataframe): Input dataframe.

    Returns:
        df(Dataframe): Dataframe with income_by_year column.

    """
    df = df.select('*',
               round((col('income_by_year') / lit(12)), 2).alias('income_by_month')
               ).drop(col('income_by_year'))

    return df


def create_premium_income_ratio_column(df: DataFrame) -> DataFrame:
    """Create premium_income_ratio column.

    Args:
        df(Dataframe): Input dataframe.

    Returns:
        df(Dataframe): Dataframe with premium_income_ratio column.

    """
    df = df.select('*',
                   round(((col('Monthly Premium Auto') / col('income_by_month'))) * 100, 2).alias('premium_income_ratio')
                   )
    return df


def create_complaint_ratio_column(df: DataFrame) -> DataFrame:
    """Create complaint_ratio_ column.

    Args:
        df(Dataframe): Input dataframe.

    Returns:
        df(Dataframe): Dataframe with complaint_ratio_ column.

    """
    df = df.select('*',
                     round(((col('Number of Open Complaints') / col('Months Since Policy Inception'))) * 100, 2).alias('complaint_rate')
                     )
    return df


def create_clv_categorical_column(df: DataFrame) -> DataFrame:
    """Create clv_categorical column.

    Args:
        df(Dataframe): Input dataframe.

    Returns:
        df(Dataframe): Dataframe with clv_categorical column.

    """
    df = df.select('*',
                   when(col('customer_lifetime_value') <= 4000, 'BAJO')
                   .when((col('customer_lifetime_value') >= 4001) & (col('customer_lifetime_value') <=6000), 'MEDIO')
                   .when(col('customer_lifetime_value') >= 6001, 'ALTO')
                   .otherwise('DESCONOCIDO').alias('clv_categorical')
                   )
    return df


def create_clv_categorical_percent_column(df: DataFrame) -> DataFrame:
    """Create clv_categorical_percent column.

    Args:
        df(Dataframe): Input dataframe.

    Returns:
        df(Dataframe): Dataframe with clv_categorical_percent column.

    """
    w = Window.partitionBy(col('clv_categorical'))
    total = df.count()

    df = df.select('*',
                count(col('clv_categorical')).over(w).alias('clv_cat_count'),
                )

    df = df.select('*',
                    round(((col('clv_cat_count') / lit(total))* 100), 1).alias('clv_categorical_percent')
                    ).drop('clv_cat_count')
    return df

def create_audit_column(df: DataFrame, load_date: date) -> DataFrame:
    """Create audit column.

    Args:
        df(Dataframe): Input dataframe.
        load_date(date): Date to create audit column.

    Returns:
        df(Dataframe): Dataframe with audit column.

    """
    df = df.select('*',
                   lit(load_date).alias('load_date')
                   )
    return df


# Transformaciones Encapsuladas
def transformations(df: DataFrame, load_date: date) -> DataFrame:
    """Orchestrate transformations functions to encapsulate all bussiness logic.

    Args:
        df(Dataframe): Input dataframe.
        load_date(date): Date used to process only correct data.

    Returns:
        df(Dataframe): Dataframe ready to write.

    """
    df = filter_and_cast(df, load_date)
    df = create_income_by_year_column(df)
    df = create_premium_income_ratio_column(df)
    df = create_complaint_ratio_column(df)
    df = create_clv_categorical_column(df)
    df = create_clv_categorical_percent_column(df)
    df = create_audit_column(df, load_date)

    return df


# Escribiendo data para consumption
def write_consumption_data(df: DataFrame,
                           path: str) -> None:
    """Write dataframe in consumption stage.

    Args:
        df(Dataframe): Input dataframe.
        path(str): Consumption S3 path.

    Returns: None.

    """
    df.coalesce(1).write.partitionBy('load_date').mode('overwrite').parquet(path)


def main():
    """Run the main Glue job logic.

    This function read data from clean stage, applies transformations
    and writes the output to S3 in partitioned parquet format.

    """
    logger.info('Starting Glue job...')
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    load_date = create_load_date()
    df_input = read_data(spark)
    data_processed = transformations(df_input, load_date)
    write_consumption_data(data_processed,
                           f'{get_consumption_s3_location()}/cons_policies')

    job.commit()
    logger.info("Job committed successfully.")


if __name__ == '__main__':
    main()
