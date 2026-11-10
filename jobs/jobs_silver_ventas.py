"""
JOB SILVER: Limpieza, deduplicación y tipado de datos (Bronze -> Silver)
Tipo: Spark (Glue)
Entrada: lakehouse_bronze_db.lakehouse_andres_2026_landing
Salida:  lakehouse_silver_db.sales_silver (Apache Iceberg)
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
print("JOB SILVER: Bronze -> Silver (Apache Iceberg)")
print("=" * 60)

# 2. SQL validado previamente en Athena
# Agregamos DROP TABLE IF EXISTS para que el job sea idempotente
# (se pueda re-ejecutar sin errores de "tabla ya existe")
silver_sql = """
DROP TABLE IF EXISTS lakehouse_silver_db.sales_silver;

CREATE TABLE lakehouse_silver_db.sales_silver
WITH (
    table_type = 'ICEBERG',
    format = 'PARQUET',
    location = 's3://lakehouse-andres-2026-silver/sales_silver/',
    is_external = false
) AS
SELECT 
    invoice_id,
    stock_code,
    description,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(invoice_date AS TIMESTAMP) AS invoice_date,
    CAST(price AS DOUBLE) AS price,
    CAST(customer_id AS INTEGER) AS customer_id,
    TRIM(UPPER(country)) AS country,
    TRIM(UPPER(department)) AS department,
    CAST(age AS INTEGER) AS age,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS processed_at
FROM (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY invoice_id, stock_code, invoice_date 
            ORDER BY invoice_date DESC
        ) AS rn
    FROM lakehouse_bronze_db.lakehouse_andres_2026_landing
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND quantity != 0
)
WHERE rn = 1
"""

# 3. Ejecutar la transformación
print("[1/2] Ejecutando query de transformación en Spark...")
spark.sql(silver_sql)
print("[2/2] Tabla Silver creada/actualizada exitosamente en Iceberg.")

print("=" * 60)
print("JOB SILVER completado exitosamente")
print("=" * 60)

# 4. Finalizar job
job.commit()