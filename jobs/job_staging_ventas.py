"""
AWS Glue Job: Staging to Bronze

Runtime: AWS Glue 4.0
Source: Amazon S3 CSV files
Target: Amazon S3 Parquet and AWS Glue Data Catalog
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F


LANDING_PATH = "s3://lakehouse-andres-2026-landing/ecommerce_raw/"
BRONZE_PATH = "s3://lakehouse-andres-2026-bronze/ecommerce_bronze"
BRONZE_DATABASE = "lakehouse_bronze_db"
BRONZE_TABLE = "ecommerce_bronze"

REQUIRED_COLUMNS = [
    "invoice_id",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "price",
    "customer_id",
    "country",
    "department",
    "age",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_columns(dataframe):
    """Validate the required source columns."""

    missing_columns = sorted(
        set(REQUIRED_COLUMNS) - set(dataframe.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )


def read_source_data(spark):
    """Read the raw CSV dataset from Amazon S3."""

    dataframe = (
        spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .csv(LANDING_PATH)
    )

    validate_columns(dataframe)

    return dataframe.select(
        *[F.col(name).cast("string") for name in REQUIRED_COLUMNS]
    )


def write_bronze_data(dataframe):
    """Write the Bronze dataset in Parquet format."""

    (
        dataframe.write
        .mode("overwrite")
        .parquet(BRONZE_PATH)
    )

    logger.info("Bronze dataset written to %s", BRONZE_PATH)


def register_bronze_table(spark):
    """Register the Bronze dataset in the Glue Data Catalog."""

    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS {BRONZE_DATABASE}
        LOCATION 's3://lakehouse-andres-2026-bronze/'
        """
    )

    spark.sql(
        f"DROP TABLE IF EXISTS {BRONZE_DATABASE}.{BRONZE_TABLE}"
    )

    schema_definition = ",\n".join(
        f"{column_name} STRING"
        for column_name in REQUIRED_COLUMNS
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE
        {BRONZE_DATABASE}.{BRONZE_TABLE} (
            {schema_definition}
        )
        STORED AS PARQUET
        LOCATION '{BRONZE_PATH}'
        """
    )


def main():
    """Execute the Staging to Bronze ETL job."""

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("Starting Staging to Bronze job")

    source_data = read_source_data(spark)
    source_count = source_data.count()

    if source_count == 0:
        raise ValueError("The source dataset is empty")

    logger.info("Source records: %s", source_count)

    write_bronze_data(source_data)
    register_bronze_table(spark)

    bronze_count = spark.table(
        f"{BRONZE_DATABASE}.{BRONZE_TABLE}"
    ).count()

    if bronze_count != source_count:
        raise RuntimeError(
            "Bronze record count does not match the source"
        )

    logger.info("Bronze records: %s", bronze_count)
    logger.info("Staging to Bronze job completed successfully")

    job.commit()
if __name__ == "__main__":
    main()