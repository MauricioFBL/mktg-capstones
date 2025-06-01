"""Glue job to get policies data from several files, clean them and create a one big table."""

import logging
import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_raw_s3_location() -> str:
    """Get the S3 location for reading table CSV files.

    Returns:
        str: S3 location.

    """
    return 's3://fcorp-data-prod/raw/policies/tables'


def get_staging_s3_location() -> str:
    """Get the S3 location for storing staging CSV file.

    Returns:
        str: S3 location.

    """
    return 's3://fcorp-data-prod/staging/policies'


def read_data(spark: SparkSession) -> DataFrame:
    """Read data from multiples tables."""
    logger.info('Reading inputs...')

    policies_types_df = (spark.read
                         .option('header', True)
                         .option('delimiter', ',')
                         .csv(f'{get_raw_s3_location()}/policies_types.csv'))

    policy_levels_df = (spark.read
                         .option('header', True)
                         .option('delimiter', ',')
                         .csv(f'{get_raw_s3_location()}/policies_levels.csv'))

    states_df = (spark.read
                 .option('header', True)
                 .option('delimiter', ',')
                 .csv(f'{get_raw_s3_location()}/states.csv'))

    transactions_df = (spark.read
                      .option('header', True)
                      .option('delimiter', ',')
                      .csv(f'{get_raw_s3_location()}/transactions.csv'))

    return policies_types_df, policy_levels_df, states_df, transactions_df


def merging_all_tables(df1: DataFrame,
                       df2: DataFrame,
                       df3: DataFrame,
                       df4: DataFrame) -> DataFrame:
    """Join 4 tables to consolid all information into a big table."""
    logger.info('Joinig inputs...')

    df = df1.join(df2,
                  on='policy_type_id',
                  how='left'
                  ).join(df3,
                         on='policy_lvl_id',
                         how='left'
                         ).join(df4,
                                on='state_id',
                                how='left'
                                )

    return df


def drop_unnecessary_columns(df: DataFrame) -> DataFrame:
    """Drop columns that are not necessary in raw stage."""
    logger.info('Cleaning data...')

    load_date = date.today()
    logger.info(f'Date process: {load_date}')

    df = df.select('*',
                   lit(load_date).alias('load_date')
                   ).drop('policy_type_id',
                          'policy_lvl_id',
                          'state_id')

    return df


def save_staging_data(df: DataFrame,
                  path: str) -> None:
    """Send data proccesed to staging stage."""
    logger.info('Writing into staging stage ...')

    df.coalesce(1).write.partitionBy('load_date').mode('append').parquet(path)


    return None


def main():
    """Entry point for Glue job."""
    logger.info('Starting Glue job...')

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    (policies_types_df,
    policy_levels_df,
    states_df,
    transactions_df) = read_data(spark)
    data_joined = merging_all_tables(df1=transactions_df,
                                     df2=policies_types_df,
                                     df3=policy_levels_df,
                                     df4=states_df)
    data_cleaned = drop_unnecessary_columns(data_joined)
    logger.info(f'Rows to write: {data_cleaned.count()}')

    save_staging_data(data_cleaned,
                      f'{get_staging_s3_location()}/stg_policies')

    job.commit()
    logger.info("Job committed successfully.")

if __name__ == '__main__':
    main()
