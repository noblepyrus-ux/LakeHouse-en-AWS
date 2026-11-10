"""
JOB STAGING: CSV -> Parquet (Landing -> Bronze)
Tipo: Spark (Glue) | Formato: Parquet
Entrada: S3 ecommerce_raw/ (CSV crudo)
Salida:  lakehouse_bronze_db.ecommerce_bronze (Parquet en Glue Catalog)
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 60)
print("JOB STAGING: CSV -> Parquet (Landing -> Bronze)")
print("=" * 60)

# 1. Crear BD Bronze con location explícita
print("[1/4] Creando base de datos Bronze...")
spark.sql("""
    CREATE DATABASE IF NOT EXISTS lakehouse_bronze_db
    LOCATION 's3://lakehouse-andres-2026-bronze/'
""")
print("      ✓ BD Bronze lista")

# 2. Leer CSV crudo DIRECTO desde S3
print("[2/4] Leyendo CSV desde S3 (ecommerce_raw/)...")
df_crudo = spark.read.option("header", "true").csv("s3://lakehouse-andres-2026-landing/ecommerce_raw/")
total_crudo = df_crudo.count()
print(f"      Filas leídas: {total_crudo}")

# 3. Escribir Parquet en S3 (Bronze)
print("[3/4] Escribiendo Parquet en S3 (ecommerce_bronze/)...")
output_path = "s3://lakehouse-andres-2026-bronze/ecommerce_bronze"
df_crudo.write.mode("overwrite").parquet(output_path)
print(f"      ✓ Datos escritos en {output_path}")

# 4. Registrar tabla con schema EXPLÍCITO (no infiere nada)
print("[4/4] Registrando tabla en Glue Catalog con schema explícito...")
spark.sql("DROP TABLE IF EXISTS lakehouse_bronze_db.ecommerce_bronze")
spark.sql("""
    CREATE TABLE lakehouse_bronze_db.ecommerce_bronze (
        invoice_id     STRING,
        stock_code     STRING,
        description    STRING,
        quantity       STRING,
        invoice_date   STRING,
        price          STRING,
        customer_id    STRING,
        country        STRING,
        department     STRING,
        age            STRING
    )
    STORED AS PARQUET
    LOCATION 's3://lakehouse-andres-2026-bronze/ecommerce_bronze'
""")
print("      ✓ Tabla Bronze registrada en catálogo")

# Verificación
count = spark.sql("SELECT COUNT(*) as total FROM lakehouse_bronze_db.ecommerce_bronze").collect()[0]["total"]
print(f"      ✓ Verificación: {count} filas en tabla Bronze")

print("=" * 60)
print("JOB STAGING completado exitosamente (CSV -> Parquet)")
print("=" * 60)

job.commit()