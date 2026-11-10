"""
JOB GOLD: Construcción del Modelo Estrella Puro (Silver -> Gold)
Tipo: Spark (Glue)
Entrada: lakehouse_silver_db.sales_silver
Salida:  lakehouse_gold_db (1 Fact + 5 Dims en Apache Iceberg)
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# 1. Inicializar contexto Glue y Spark
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 60)
print("JOB GOLD: Silver -> Modelo Estrella (Apache Iceberg)")
print("=" * 60)

# 2. Asegurar que el esquema Gold existe
print("[0/7] Creando schema gold si no existe...")
spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse_gold_db")

# 3. Lista de queries SQL validadas (con DROP IF EXISTS para idempotencia)
queries = [
    # Dim 1: DATE
    (1, "DIM_DATE", """
        DROP TABLE IF EXISTS lakehouse_gold_db.dim_date;
        CREATE TABLE lakehouse_gold_db.dim_date
        WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_date/', is_external = false) AS
        SELECT
            CAST(YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d) AS INTEGER) AS date_key,
            d AS full_date, YEAR(d) AS year, QUARTER(d) AS quarter, MONTH(d) AS month,
            DATE_FORMAT(d, '%M') AS month_name, DAY(d) AS day_of_month,
            DAY_OF_WEEK(d) AS day_of_week, DATE_FORMAT(d, '%W') AS day_name,
            CASE WHEN DAY_OF_WEEK(d) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
            FALSE AS is_holiday, WEEK(d) AS week_of_year,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
        FROM (SELECT CAST(t AS DATE) AS d FROM UNNEST(SEQUENCE(DATE '2024-01-01', DATE '2027-12-31', INTERVAL '1' DAY)) AS x(t))
    """),
    
    # Dim 2: PRODUCT
    (2, "DIM_PRODUCT", """
        DROP TABLE IF EXISTS lakehouse_gold_db.dim_product;
        CREATE TABLE lakehouse_gold_db.dim_product
        WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_product/', is_external = false) AS
        SELECT stock_code, description, department,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
        FROM lakehouse_silver_db.sales_silver GROUP BY stock_code, description, department
    """),

    # Dim 3: CUSTOMER
    (3, "DIM_CUSTOMER", """
        DROP TABLE IF EXISTS lakehouse_gold_db.dim_customer;
        CREATE TABLE lakehouse_gold_db.dim_customer
        WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_customer/', is_external = false) AS
        SELECT customer_id, age,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
        FROM lakehouse_silver_db.sales_silver GROUP BY customer_id, age
    """),

    # Dim 4: DEPARTMENT
    (4, "DIM_DEPARTMENT", """
        DROP TABLE IF EXISTS lakehouse_gold_db.dim_department;
        CREATE TABLE lakehouse_gold_db.dim_department
        WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_department/', is_external = false) AS
        SELECT department,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
        FROM lakehouse_silver_db.sales_silver WHERE department IS NOT NULL AND department != '' GROUP BY department
    """),

    # Dim 5: COUNTRY
    (5, "DIM_COUNTRY", """
        DROP TABLE IF EXISTS lakehouse_gold_db.dim_country;
        CREATE TABLE lakehouse_gold_db.dim_country
        WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_country/', is_external = false) AS
        SELECT country,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
        FROM lakehouse_silver_db.sales_silver WHERE country IS NOT NULL AND country != '' GROUP BY country
    """),

    # Fact: SALES
    (6, "FACT_SALES", """
        DROP TABLE IF EXISTS lakehouse_gold_db.fact_sales;
        CREATE TABLE lakehouse_gold_db.fact_sales
        WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/fact_sales/', is_external = false) AS
        SELECT 
            invoice_id,
            stock_code,
            customer_id,
            CAST(YEAR(invoice_date) * 10000 + MONTH(invoice_date) * 100 + DAY(invoice_date) AS INTEGER) AS date_key,
            department,
            country,
            quantity,
            price AS unit_price,
            CAST(quantity AS DOUBLE) * price AS revenue,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
        FROM lakehouse_silver_db.sales_silver 
        WHERE invoice_id IS NOT NULL AND invoice_id != ''
    """)
]

# 4. Ejecutar cada query secuencialmente
for step, table_name, sql_query in queries:
    print(f"[{step}/6] Procesando {table_name}...")
    spark.sql(sql_query)
    print(f"      ✓ {table_name} creada/actualizada exitosamente.")

print("=" * 60)
print("JOB GOLD completado exitosamente. Modelo Estrella listo.")
print("=" * 60)

# 5. Finalizar job
job.commit()