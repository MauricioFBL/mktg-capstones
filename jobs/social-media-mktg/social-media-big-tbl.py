"""Glue job to process campaign, ad group, and daily metrics from CSV files."""

import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, lit, split

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""Glue job to process campaign, ad group, and daily metrics from CSV files."""


def read_inputs(spark: SparkSession, base_path: str = "./inputs") -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Read campaign, ad group, ad, and daily data CSV files.

    Args:
        spark: Active Spark session.
        base_path: Directory containing input CSVs.

    Returns:
        A tuple of DataFrames: (campaign_df, group_df, ads_df, daily_df)

    """
    logger.info("Reading input CSV files...")
    campaign_df = spark.read.csv(
        f"{base_path}/campaigns.csv", header=True, inferSchema=True)
    group_df = spark.read.csv(
        f"{base_path}/ad_groups.csv", header=True, inferSchema=True)
    ads_df = spark.read.csv(f"{base_path}/ads.csv",
                            header=True, inferSchema=True)
    daily_df = spark.read.csv(
        f"{base_path}/daily_data.csv", header=True, inferSchema=True)

    return campaign_df, group_df, ads_df, daily_df

    logger.info("Transforming campaigns DataFrame...")


def transform_campaigns(df: DataFrame) -> DataFrame:
    """Transform the campaigns DataFrame by splitting and renaming columns.

    Args:
        df: Original campaign DataFrame.

    Returns:
        Transformed DataFrame.

    """
    df = df.withColumn("prefijo", split(df["Campaign_Name"], "-")[0])
    df = df.withColumn("id", split(df["Campaign_Name"], "-")[1])
    df = df.withColumn("marca", split(df["Campaign_Name"], "-")[2])
    df = df.withColumn("tipo", split(df["Campaign_Name"], "-")[3])
    df = df.withColumn("campaign_name", concat(
        col("prefijo"), lit("_"), col("id")))
    df = df.drop("Campaign_Name", "prefijo")

    return df.withColumnRenamed("Campaign_ID", "campaign_id")

    logger.info("Transforming ad groups DataFrame...")


def transform_ad_groups(df: DataFrame) -> DataFrame:
    """Transform the ad groups DataFrame by extracting structured fields.

    Args:
        df: Original ad groups DataFrame.

    Returns:
        Transformed DataFrame.

    """
    df = df.withColumnRenamed("Ad_Group_ID", "ad_group_id")
    df = df.withColumnRenamed("Campaign_ID", "campaign_id")
    df = df.withColumn("id", split(df["Ad_Group_Name"], "-")[1])
    df = df.withColumn("audience", split(df["Ad_Group_Name"], "-")[2])
    df = df.withColumn("prefijo", split(df["Ad_Group_Name"], "-")[0])
    df = df.withColumn("ad_group_name", concat(
        col("prefijo"), lit("_"), col("id")))

    return df.drop("prefijo")

    logger.info("Transforming ads DataFrame...")


def transform_ads(df: DataFrame) -> DataFrame:
    """Transform the ads DataFrame by normalizing and splitting fields.

    Args:
        df: Original ads DataFrame.

    Returns:
        Transformed ads DataFrame.

    """
    df = df.withColumnRenamed("Ad_ID", "ad_id")
    df = df.withColumnRenamed("Ad_Group_ID", "ad_group_id")
    df = df.withColumn("id", split(df["Ad_Name"], "-")[1])
    df = df.withColumn("prefijo", split(df["Ad_Name"], "-")[0])
    df = df.withColumn("ad_type", split(df["Ad_Name"], "-")[2])
    df = df.withColumn("ad_name", concat(col("prefijo"), lit("_"), col("id")))

    return df.drop("prefijo")

    logger.info("Joining and cleaning all DataFrames...")


def join_and_clean(
    daily_df: DataFrame,
    ads_df: DataFrame,
    group_df: DataFrame,
    campaign_df: DataFrame,
) -> DataFrame:
    """Perform joins across all DataFrames and normalize column names.

    Args:
        daily_df: Daily metrics.
        ads_df: Ads metadata.
        group_df: Ad groups metadata.
        campaign_df: Campaigns metadata.

    Returns:
        Unified and cleaned DataFrame.

    """
    df = daily_df.join(ads_df, ["ad_id", "Platform"], "left")
    df = df.join(group_df, ["ad_group_id"], "left")
    df = df.join(campaign_df, ["campaign_id"], "left")

    cols_dict = {c: c.replace(" ", "_").lower() for c in df.columns}
    for old, new in cols_dict.items():
        df = df.withColumnRenamed(old, new)

    final_cols = [
        "campaign_id", "ad_group_id", "ad_group_name", "ad_id", "ad_name", "platform", "date",
        "marca", "tipo", "audience", "ad_type", "impressions", "clicks", "spend",
        "interactions", "conversions", "quartile_25", "quartile_50", "quartile_75", "completed"
    ]
    return df.select([c for c in final_cols if c in df.columns])


def write_output(df: DataFrame, path: str = "./outputs/mfbl/daily_data.csv") -> None:
    """Write the DataFrame to a single CSV file.

    Args:
        df: Data to be written.
        path: Output path.

    """
    logger.info(f"Writing output to {path}")
    df.coalesce(1).write.mode("overwrite").csv(path, header=True)


def main() -> None:
    """Entry point for Glue job."""
    logger.info("Starting Glue job...")
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    campaign_df, group_df, ads_df, daily_df = read_inputs(spark)
    campaign_df = transform_campaigns(campaign_df)
    group_df = transform_ad_groups(group_df)
    ads_df = transform_ads(ads_df)
    result_df = join_and_clean(daily_df, ads_df, group_df, campaign_df)

    write_output(result_df, "s3://fcorp-data-prod/staging/your_output/")
    logger.info("Job committed successfully.")
    job.commit()


if __name__ == "__main__":
    main()
