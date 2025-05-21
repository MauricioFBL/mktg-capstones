"""Glue job to get policies data from several files, clean them and create a one big table."""

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


def create_load_date():
     """Get today date
     
     Returns: Date: Audit date
     """
     return date.today()



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
    df = spark.read.option('header', True).csv(f'{get_staging_s3_location()}/policies_stg.csv')
    return df

# Bussiness Logic - Transformations
def filter_and_cast(df: DataFrame, partition_date: date) -> DataFrame:

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
    df = df.select('*',
               round((col('income_by_year') / lit(12)), 2).alias('income_by_month')
               ).drop(col('income_by_year'))

    return df


def create_premium_income_ratio_column(df: DataFrame) -> DataFrame:
    df = df.select('*',
                   round(((col('Monthly Premium Auto') / col('income_by_month'))) * 100, 2).alias('premium_income_ratio')
                   )
    return df


def create_complaint_ratio_column(df: DataFrame) -> DataFrame:
    df = df.select('*',
                     round(((col('Number of Open Complaints') / col('Months Since Policy Inception'))) * 100, 2).alias('complaint_rate')
                     )
    return df


def create_clv_categorical_column(df: DataFrame) -> DataFrame:
    df = df.select('*',
                   when(col('customer_lifetime_value') <= 4000, 'BAJO')
                   .when((col('customer_lifetime_value') >= 4001) & (col('customer_lifetime_value') <=6000), 'MEDIO')
                   .when(col('customer_lifetime_value') >= 6001, 'ALTO')
                   .otherwise('DESCONOCIDO').alias('clv_categorical')
                   )
    return df


def create_clv_categorical_percent_column(df: DataFrame) -> DataFrame:
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
    df = df.select('*',
                   lit(load_date).alias('load_date')
                   )
    return df


# Transformaciones Encapsuladas
def transformations(df: DataFrame, load_date: date) -> DataFrame:
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

    df.coalesce(1).write.partitionBy('load_date').mode('overwrite').parquet(path)


def main():
    """Entry point for Glue job."""
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
