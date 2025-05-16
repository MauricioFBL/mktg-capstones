"""Process and aggregate Meta marketing data for monthly consumption reporting."""

import sys
from logging import Logger

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, month, sum, to_date, year

# ========= Funciones =========


def read_input_data(glue_context: GlueContext, input_path: str,
                    logger: Logger) -> DynamicFrame:
    """Read CSV data from S3 into a DynamicFrame.

    Args:
        glue_context (GlueContext): Active Glue context.
        input_path (str): S3 input path.
        logger (Logger): Logger instance from the Glue context.

    Returns:
        DynamicFrame: Raw data.

    """
    logger.info(f"Reading input data from {input_path}")
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [input_path]},
        format="csv",
        format_options={"withHeader": True}
    )
    logger.info(f"Input data read successfully with"
                f"{dyf.count()} records.")
    return dyf


def transform_data(dyf: DynamicFrame, glue_context: GlueContext,
                   logger: Logger) -> DynamicFrame:
    """Clean, enrich and aggregate the data.

    Args:
        dyf (DynamicFrame): Input DynamicFrame.
        glue_context (GlueContext): Glue context for conversion.
        logger (Logger): Logger instance from the Glue context.

    Returns:
        DynamicFrame: Aggregated and partition-ready data.

    """
    logger.info("Starting data transformation")
    df = dyf.toDF()

    df = df.withColumn("date_parsed", to_date(col("date"))) \
           .withColumn("year", year("date_parsed")) \
           .withColumn("month", month("date_parsed"))

    df = df.filter(
        (col("impressions") > 0) &
        (col("spend") >= 0) &
        (col("clicks") >= 0)
    )

    df_agg = df.groupBy(
        "campaign_id", "ad_group_name", "ad_name", "platform",
        "marca", "tipo", "audience", "ad_type", "year", "month"
    ).agg(
        sum("impressions").alias("impressions"),
        sum("clicks").alias("clicks"),
        sum("spend").alias("spend"),
        sum("conversions").alias("conversions"),
        sum("completed").alias("completed")
    )

    logger.info(f"Transformation complete. Aggregated record count:"
                f"{df_agg.count()}")
    return DynamicFrame.fromDF(df_agg, glue_context, "df_agg")


def write_parquet_partitioned(
        dyf: DynamicFrame, output_path: str,
        glue_context: GlueContext, logger: Logger) -> None:
    """Write a DynamicFrame to S3 as partitioned Parquet.

    Args:
        dyf (DynamicFrame): Data to write.
        output_path (str): S3 output path.
        glue_context (GlueContext): Active Glue context.
        logger (Logger): Logger instance from the Glue context.

    """
    logger.info(f"Writing output data to {output_path}, "
                f"partitioned by year and month")
    glue_context.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["year", "month"]
        },
        format="parquet"
    )
    logger.info("Write complete.")


def main() -> None:
    """Run the main Glue job logic.

    This function reads input parameters, initializes the Glue context and logger,
    processes the input data, applies transformations, and writes the output to S3
    in partitioned Parquet format.
    """
    # ========= Argumentos =========
    args = getResolvedOptions(sys.argv, ["INPUT_PATH", "OUTPUT_PATH"])
    INPUT_PATH = args["INPUT_PATH"]
    OUTPUT_PATH = args["OUTPUT_PATH"]

    # ========= Contextos y Logger =========
    sc = SparkContext()
    glue_context = GlueContext(sc)
    logger = glue_context.get_logger()
    logger.info(f"Job started. Input: {INPUT_PATH}, "
                f"Output: {OUTPUT_PATH}")

    # ========= Ejecución principal =========
    raw_data = read_input_data(glue_context, INPUT_PATH, logger)
    transformed_data = transform_data(raw_data, glue_context, logger)
    write_parquet_partitioned(transformed_data, OUTPUT_PATH,
                              glue_context, logger)
    logger.info("Job completed successfully.")


if __name__ == "__main__":
    main()
