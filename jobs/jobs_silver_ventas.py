"""
JOB SILVER: Bronze -> Silver (Parquet 100% Spark-native)
Sin Iceberg. Sin saveAsTable. Sin warehouse. Sin metadatos de Athena.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, trim, upper, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 60)
print("JOB SILVER: Bronze -> Silver (Parquet puro)")
print("=" * 60)

# 1. Crear BD con ubicación EXPLÍCITA (elimina "empty string path")
print("[1/5] Creando base de datos con location explícita...")
spark.sql("""
    CREATE DATABASE IF NOT EXISTS lakehouse_silver_db
    LOCATION 's3://lakehouse-andres-2026-silver/'
""")
print("      ✓ BD creada con location válida")

# 2. Leer Parquet DIRECTO desde S3 (ignora catálogo, ignora particiones)
print("[2/5] Leyendo Parquet desde S3...")
df_bronze = spark.read.parquet("s3://lakehouse-andres-2026-landing/ecommerce_raw/")
total_bronze = df_bronze.count()
print(f"      Filas leídas: {total_bronze}")

# 3. Transformación: limpieza, cast, deduplicación
print("[3/5] Aplicando limpieza y tipado...")
window_spec = Window.partitionBy("invoice_id", "stock_code", "invoice_date").orderBy(col("invoice_date").desc())

df_silver = (
    df_bronze
    .filter((col("invoice_id").isNotNull()) & (col("invoice_id") != "") & (col("quantity") != 0))
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .select(
        col("invoice_id"),
        col("stock_code"),
        col("description"),
        col("quantity").cast("int").alias("quantity"),
        col("invoice_date").cast("timestamp").alias("invoice_date"),
        col("price").cast("double").alias("price"),
        col("customer_id").cast("int").alias("customer_id"),
        trim(upper(col("country"))).alias("country"),
        trim(upper(col("department"))).alias("department"),
        col("age").cast("int").alias("age"),
        current_timestamp().alias("processed_at")
    )
    .drop("rn")
)
total_silver = df_silver.count()
print(f"      Filas limpias: {total_silver}")

# 4. Escribir Parquet a ruta EXPLÍCITA (no usa warehouse, no usa saveAsTable)
print("[4/5] Escribiendo Parquet a S3...")
output_path = "s3://lakehouse-andres-2026-silver/sales_silver"
df_silver.write.mode("overwrite").parquet(output_path)
print(f"      ✓ Datos escritos en {output_path}")

# 5. Registrar tabla con schema EXPLÍCITO (no infiere nada, no lee S3)
print("[5/5] Registrando tabla en Glue Catalog con schema explícito...")
spark.sql("DROP TABLE IF EXISTS lakehouse_silver_db.sales_silver")
spark.sql("""
    CREATE TABLE lakehouse_silver_db.sales_silver (
        invoice_id     STRING,
        stock_code     STRING,
        description    STRING,
        quantity       INT,
        invoice_date   TIMESTAMP,
        price          DOUBLE,
        customer_id    INT,
        country        STRING,
        department     STRING,
        age            INT,
        processed_at   TIMESTAMP
    )
    STORED AS PARQUET
    LOCATION 's3://lakehouse-andres-2026-silver/sales_silver'
""")
print("      ✓ Tabla registrada en catálogo")

# Verificación
count = spark.sql("SELECT COUNT(*) as total FROM lakehouse_silver_db.sales_silver").collect()[0]["total"]
print(f"      ✓ Verificación: {count} filas en tabla Silver")

print("=" * 60)
print("JOB SILVER completado exitosamente")
print("=" * 60)

job.commit()