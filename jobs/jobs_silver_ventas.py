"""
AWS Glue Job: Bronze to Silver

Runtime: AWS Glue 4.0
Source: lakehouse_bronze_db.ecommerce_bronze
Target: lakehouse_silver_db.sales_silver
Format: Apache Parquet
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F


BRONZE_TABLE = "lakehouse_bronze_db.ecommerce_bronze"
SILVER_DATABASE = "lakehouse_silver_db"
SILVER_TABLE = "sales_silver"
SILVER_PATH = "s3://lakehouse-andres-2026-silver/sales_silver"

SOURCE_COLUMNS = [
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
    """Validate the required Bronze columns."""

    missing_columns = sorted(
        set(SOURCE_COLUMNS) - set(dataframe.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing Bronze columns: {missing_columns}"
        )


def transform_sales_data(dataframe):
    """Clean, validate, and deduplicate Bronze sales."""

    validate_columns(dataframe)

    cleaned_data = dataframe.select(
        F.trim(F.col("invoice_id")).alias("invoice_id"),
        F.trim(F.col("stock_code")).alias("stock_code"),
        F.trim(F.col("description")).alias("description"),
        F.col("quantity").cast("int").alias("quantity"),
        F.to_timestamp(
            F.trim(F.col("invoice_date")),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("invoice_date"),
        F.col("price").cast("decimal(18, 2)").alias("price"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        F.upper(F.trim(F.col("department"))).alias("department"),
        F.col("age").cast("int").alias("age"),
    )

    valid_data = cleaned_data.filter(
        F.col("invoice_id").isNotNull()
        & (F.col("invoice_id") != "")
        & F.col("stock_code").isNotNull()
        & (F.col("stock_code") != "")
        & F.col("quantity").isNotNull()
        & (F.col("quantity") != 0)
        & F.col("invoice_date").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") >= 0)
    )

    return (
        valid_data
        .dropDuplicates(SOURCE_COLUMNS)
        .withColumn("processed_at", F.current_timestamp())
    )


def write_silver_data(dataframe):
    """Write validated sales records to Amazon S3."""

    (
        dataframe.write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )


def register_silver_table(spark):
    """Register the Silver dataset in the Glue Data Catalog."""

    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}
        LOCATION 's3://lakehouse-andres-2026-silver/'
        """
    )

    spark.sql(
        f"DROP TABLE IF EXISTS {SILVER_DATABASE}.{SILVER_TABLE}"
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE
        {SILVER_DATABASE}.{SILVER_TABLE} (
            invoice_id STRING,
            stock_code STRING,
            description STRING,
            quantity INT,
            invoice_date TIMESTAMP,
            price DECIMAL(18, 2),
            customer_id INT,
            country STRING,
            department STRING,
            age INT,
            processed_at TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{SILVER_PATH}'
        """
    )


def main():
    """Execute the Bronze to Silver ETL job."""

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("Starting Bronze to Silver job")

    bronze_data = spark.table(BRONZE_TABLE)
    bronze_count = bronze_data.count()

    if bronze_count == 0:
        raise ValueError("The Bronze dataset is empty")

    silver_data = transform_sales_data(bronze_data)
    silver_count = silver_data.count()

    if silver_count == 0:
        raise ValueError("No valid Silver records were produced")

    logger.info("Bronze records: %s", bronze_count)
    logger.info("Validated Silver records: %s", silver_count)
    logger.info(
        "Filtered or deduplicated records: %s",
        bronze_count - silver_count
    )

    write_silver_data(silver_data)
    register_silver_table(spark)

    registered_count = spark.table(
        f"{SILVER_DATABASE}.{SILVER_TABLE}"
    ).count()

    if registered_count != silver_count:
        raise RuntimeError(
            "Silver record count validation failed"
        )

    logger.info("Silver records: %s", registered_count)
    logger.info("Bronze to Silver job completed successfully")

    job.commit()

if __name__ == "__main__":
    main()