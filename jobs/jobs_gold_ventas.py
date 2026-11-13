"""
AWS Glue Job: Silver to Gold

Runtime: AWS Glue 4.0
Source: lakehouse_silver_db.sales_silver
Target: lakehouse_gold_db
Format: Apache Parquet
Data Model:
    dim_date
    dim_product
    dim_customer
    dim_department
    dim_country
    fact_sales
"""
import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

SILVER_TABLE = "lakehouse_silver_db.sales_silver"
GOLD_DATABASE = "lakehouse_gold_db"
GOLD_BUCKET = "s3://lakehouse-andres-2026-gold"
GOLD_TABLES = [
    "dim_date",
    "dim_product",
    "dim_customer",
    "dim_department",
    "dim_country",
    "fact_sales",
]
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_gold_database(spark):
    """Create the Gold database if it does not exist."""
    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}
        LOCATION '{GOLD_BUCKET}/'
        """
    )
def validate_silver_data(dataframe):
    """Validate the Silver schema and required values."""
    required_columns = {
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
    }
    missing_columns = sorted(
        required_columns - set(dataframe.columns)
    )
    if missing_columns:
        raise ValueError(
            f"Missing Silver columns: {missing_columns}"
        )
    invalid_records = dataframe.filter(
        F.col("invoice_id").isNull()
        | (F.trim(F.col("invoice_id")) == "")
        | F.col("stock_code").isNull()
        | (F.trim(F.col("stock_code")) == "")
        | F.col("invoice_date").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") == 0)
        | F.col("price").isNull()
    )
    if invalid_records.limit(1).count() > 0:
        raise ValueError(
            "Silver contains records with missing required values"
        )
def add_audit_columns(dataframe):
    """Add processing timestamps to the dataset."""
    return (
        dataframe
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

def create_date_dimension(sales):
    """Create a date dimension from available sales dates."""
    dates = (
        sales
        .select(
            F.to_date("invoice_date").alias("full_date")
        )
        .filter(F.col("full_date").isNotNull())
        .distinct()
    )

    result = dates.select(
        F.date_format("full_date", "yyyyMMdd")
        .cast("int")
        .alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM")
        .alias("month_name"),
        F.dayofmonth("full_date")
        .alias("day_of_month"),
        F.dayofweek("full_date")
        .alias("day_of_week"),
        F.date_format("full_date", "EEEE")
        .alias("day_name"),
        F.dayofweek("full_date")
        .isin(1, 7)
        .alias("is_weekend"),
        F.weekofyear("full_date")
        .alias("week_of_year"),
    )
    return add_audit_columns(result)
    
def create_product_dimension(sales):
    """Create one product record per stock code."""
    result = (
        sales
        .filter(
            F.col("stock_code").isNotNull()
            & (F.col("stock_code") != "")
        )
        .groupBy("stock_code")
        .agg(
            F.min("description").alias("description"),
            F.min("department").alias("department"),
        )
    )
    return add_audit_columns(result)
    
def create_customer_dimension(sales):
    """Create one customer record per customer identifier."""
    result = (
        sales
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(F.max("age").alias("age"))
    )
    return add_audit_columns(result)
    
def create_department_dimension(sales):
    """Create the department dimension."""
    result = (
        sales
        .select("department")
        .filter(
            F.col("department").isNotNull()
            & (F.col("department") != "")
        )
        .distinct()
    )
    return add_audit_columns(result)

def create_country_dimension(sales):
    """Create the country dimension."""

    result = (
        sales
        .select("country")
        .filter(
            F.col("country").isNotNull()
            & (F.col("country") != "")
        )
        .distinct()
    )
    return add_audit_columns(result)
    
def create_sales_fact(sales):
    """Create the sales fact table."""
    result = sales.select(
        F.col("invoice_id"),
        F.col("stock_code"),
        F.col("customer_id"),
        F.date_format(
            "invoice_date", "yyyyMMdd"
        ).cast("int").alias("date_key"),
        F.col("department"),
        F.col("country"),
        F.col("quantity").cast("int"),
        F.col("price")
        .cast("decimal(18, 2)")
        .alias("unit_price"),
        (
            F.col("quantity").cast("decimal(18, 0)")
            * F.col("price").cast("decimal(18, 2)")
        ).cast("decimal(28, 2)").alias("revenue"),
    )
    return add_audit_columns(result)
    
def register_gold_table(spark, table_name, dataframe):
    """Write a Gold dataset and register its external table."""
    table_path = f"{GOLD_BUCKET}/{table_name}"
    full_table_name = f"{GOLD_DATABASE}.{table_name}"
    logger.info("Writing Gold table: %s", table_name)
    (
        dataframe.write
        .mode("overwrite")
        .parquet(table_path)
    )
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    schema_definition = ",\n".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        for field in dataframe.schema.fields
    )
    spark.sql(
    f"""
    CREATE TABLE {full_table_name} (
        {schema_definition}
    )
    USING PARQUET
    LOCATION '{table_path}'
    """
    )
    logger.info("Registered Gold table: %s", full_table_name)

def validate_gold_tables(spark, expected_fact_count):
    """Validate the registered Gold tables."""
    for table_name in GOLD_TABLES:
        full_name = f"{GOLD_DATABASE}.{table_name}"
        record_count = spark.table(full_name).count()
        logger.info(
            "Gold table %s contains %s records",
            table_name,
            record_count
        )
        if record_count == 0:
            raise RuntimeError(
                f"Gold table {table_name} is empty"
            )
        if (
            table_name == "fact_sales"
            and record_count != expected_fact_count
        ):
            raise RuntimeError(
                "Gold fact record count does not match Silver"
            )
def validate_dimension_keys(spark):
    """Validate uniqueness of Gold dimension keys."""
    dimensions = {
        "dim_date": "date_key",
        "dim_product": "stock_code",
        "dim_customer": "customer_id",
        "dim_department": "department",
        "dim_country": "country",
    }
    for table_name, key_column in dimensions.items():
        dataframe = spark.table(
            f"{GOLD_DATABASE}.{table_name}"
        )
        duplicate_keys = (
            dataframe.groupBy(key_column)
            .count()
            .filter(F.col("count") > 1)
        )
        if duplicate_keys.limit(1).count() > 0:
            raise ValueError(
                f"Duplicate keys found in {table_name}"
            )
        logger.info(
            "Dimension key validation passed: %s",
            table_name
        )
def main():
    """Execute the Silver to Gold ETL job."""
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    logger.info("Starting Silver to Gold job")
    initialize_gold_database(spark)
    silver_data = spark.table(SILVER_TABLE)
    silver_count = silver_data.count()
    if silver_count == 0:
        raise ValueError("The Silver dataset is empty")
    validate_silver_data(silver_data)
    logger.info("Silver records: %s", silver_count)
    gold_datasets = {
        "dim_date": create_date_dimension(silver_data),
        "dim_product": create_product_dimension(silver_data),
        "dim_customer": create_customer_dimension(silver_data),
        "dim_department": create_department_dimension(silver_data),
        "dim_country": create_country_dimension(silver_data),
        "fact_sales": create_sales_fact(silver_data),
    }

    for table_name in GOLD_TABLES:
        register_gold_table(
            spark,
            table_name,
            gold_datasets[table_name]
        )
    validate_gold_tables(spark, silver_count)
    validate_dimension_keys(spark)

    logger.info("Silver to Gold job completed successfully")

    job.commit()
if __name__ == "__main__":
    main()