"""Process and aggregate Meta marketing data for monthly consumption reporting."""

import sys
from logging import Logger

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import avg, col, expr, month, sum, to_date, year
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import LongType

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


def define_correct_types(df: DataFrame, cols_map: dict,
                         logger: Logger) -> DataFrame:
    """Convert DataFrame columns to the correct types.

    Args:
        df (DataFrame): Input DataFrame.
        cols_map (dict): Dictionary mapping column names to types.
        logger (Logger): Logger instance from the Glue context.

    Returns:
        DataFrame: DataFrame with converted column types.

    """
    for col_name, col_type in cols_map.items():
        if col_type == "int":
            df = df.withColumn(col_name, col(col_name).cast("int"))
        elif col_type == "long":
            df = df.withColumn(col_name, col(col_name).cast(LongType()))
        elif col_type == "double":
            df = df.withColumn(col_name, col(col_name).cast("double"))
        elif col_type == "string":
            df = df.withColumn(col_name, col(col_name).cast("string"))

    logger.info("Data types converted successfully.")
    return df


def transform_data(dyf: DynamicFrame,
                   logger: Logger) -> DataFrame:
    """Clean, enrich and aggregate the data.

    Args:
        dyf (DynamicFrame): Input DynamicFrame.
        glue_context (GlueContext): Glue context for conversion.
        logger (Logger): Logger instance from the Glue context.

    Returns:
        DataFrame: Aggregated and partition-ready data.

    """
    logger.info("Starting data transformation")
    df = dyf.toDF()

    df = df.withColumn("date_parsed", to_date(col("date"))) \
           .withColumn("year", year("date_parsed")) \
           .withColumn("month", month("date_parsed"))

    df = df.withColumn("campaign_id", col("campaign_id").cast("int"))

    df = df.filter(
        (col("impressions") > 0) &
        (col("spend") >= 0) &
        (col("clicks") >= 0)
    )
    conversion_types = {
        "campaign_id": "string",
        "ad_group_name": "string",
        "ad_name": "string",
        "platform": "string",
        "marca": "string",
        "tipo": "string",
        "audience": "string",
        "ad_type": "string",
        "year": "int",
        "month": "int",
        "impressions": "long",
        "clicks": "long",
        "spend": "double",
        "conversions": "long",
        "completed": "long"
    }

    df = define_correct_types(df, conversion_types, logger)

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
    return df_agg


def calculate_metrics(df: DataFrame, logger: Logger) -> DataFrame:
    """Calculate additional metrics for the DataFrame.

    Args:
        df (DataFrame): Input DataFrame.
        logger (Logger): Logger instance from the Glue context.

    Returns:
        DataFrame: DataFrame with additional metrics.

    """
    df = df.withColumn("CTR", col("clicks") / col("impressions")) \
           .withColumn("CPC", col("spend") / col("clicks")) \
           .withColumn("CPA", col("spend") / col("conversions")) \
           .withColumn("ROI", (col("conversions") * 100) / col("spend")) \
           .withColumn("CPM", (col("spend") * 1000) / col("impressions")) \
           .withColumn("completion_rate", col("completed") / col("impressions"))
    logger.info("Extra Metrics calculated.")
    return df


def create_kpis_from_3m(df: DataFrame, glue_context: GlueContext,
                        logger: Logger) -> DataFrame:
    """Create KPIs from the last 3 months of data.

    Args:
        df (DataFrame): Input DataFrame.
        glue_context (GlueContext): Glue context for conversion.
        logger (Logger): Logger instance from the Glue context.

    Returns:
        DataFrame: DataFrame with KPIs from the last 3 months.

    """
    # Definir ventana de 3 meses móviles
    window_spec = Window.partitionBy("platform", "marca", "tipo", "audience", "ad_type")\
                        .orderBy("year", "month")\
                        .rowsBetween(-2, 0)

    # 8. Métricas móviles
    final_df = df.withColumn("CTR_3m", avg("CTR").over(window_spec))\
        .withColumn("CPA_3m", avg("CPA").over(window_spec))\
        .withColumn("ROI_3m", avg("ROI").over(window_spec))\
        .withColumn("spend_3m", sum("spend").over(window_spec))\
        .withColumn("conversions_3m", sum("conversions").over(window_spec))

    final_df = final_df.withColumn("CTR_3m", spark_round(col("CTR_3m"), 4))\
        .withColumn("CPA_3m", spark_round(col("CPA_3m"), 4))\
        .withColumn("ROI_3m", spark_round(col("ROI_3m"), 4))\
        .withColumn("spend_3m", spark_round(col("spend_3m"), 2))\
        .withColumn("conversions_3m", spark_round(col("conversions_3m"), 2))

    final_df = final_df.withColumn(
        "last_updated_date", expr("date_sub(current_date(), 1)"))
    logger.info("KPIs from the last 3 months calculated.")
    return DynamicFrame.fromDF(final_df, glue_context, "final_df")


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
    transformed_data = transform_data(raw_data, logger)
    del raw_data
    transformed_data = calculate_metrics(transformed_data, logger)
    transformed_data = create_kpis_from_3m(transformed_data,
                                           glue_context, logger)
    write_parquet_partitioned(transformed_data, OUTPUT_PATH,
                              glue_context, logger)
    logger.info("Job completed successfully.")


if __name__ == "__main__":
    main()
