"""
JOB GOLD: Modelo Estrella Puro (Silver -> Gold)
Tipo: Spark (Glue) | Formato: Parquet (patrón ganador del Silver)
Entrada: S3 sales_silver/ (Parquet de Silver)
Salida:  lakehouse_gold_db (1 Fact + 5 Dims en Parquet)
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 60)
print("JOB GOLD: Silver -> Modelo Estrella (Parquet)")
print("=" * 60)

# Helper: aplica el patrón ganador a cada tabla (write + register con schema explícito)
def register_table(table_name, path, df, schema_sql):
    df.write.mode("overwrite").parquet(path)
    spark.sql(f"DROP TABLE IF EXISTS lakehouse_gold_db.{table_name}")
    spark.sql(f"""
        CREATE TABLE lakehouse_gold_db.{table_name} ({schema_sql})
        STORED AS PARQUET
        LOCATION '{path}'
    """)
    print(f"      ✓ {table_name} creada y registrada")

# 1. BD Gold con location explícita
print("[1/8] Creando base de datos Gold...")
spark.sql("""
    CREATE DATABASE IF NOT EXISTS lakehouse_gold_db
    LOCATION 's3://lakehouse-andres-2026-gold/'
""")
print("      ✓ BD Gold lista")

# 2. Leer Silver directo de S3 y crear vista
print("[2/8] Leyendo Silver desde S3...")
df_silver = spark.read.parquet("s3://lakehouse-andres-2026-silver/sales_silver")
df_silver.createOrReplaceTempView("silver_view")
print(f"      Filas Silver: {df_silver.count()}")

# 3. DIM_DATE (generada por rango de fechas)
print("[3/8] Generando DIM_DATE...")
df_dim_date = spark.sql("""
    SELECT
        CAST(year(d)*10000 + month(d)*100 + day(d) AS INT) AS date_key,
        d AS full_date,
        year(d) AS year,
        quarter(d) AS quarter,
        month(d) AS month,
        date_format(d, 'MMMM') AS month_name,
        day(d) AS day_of_month,
        dayofweek(d) AS day_of_week,
        date_format(d, 'EEEE') AS day_name,
        CASE WHEN dayofweek(d) IN (1,7) THEN true ELSE false END AS is_weekend,
        false AS is_holiday,
        weekofyear(d) AS week_of_year,
        current_timestamp() AS created_at,
        current_timestamp() AS updated_at
    FROM (SELECT explode(sequence(to_date('2024-01-01'), to_date('2027-12-31'), interval 1 day)) AS d)
""")
register_table("dim_date", "s3://lakehouse-andres-2026-gold/dim_date", df_dim_date, """
    date_key INT, full_date DATE, year INT, quarter INT, month INT,
    month_name STRING, day_of_month INT, day_of_week INT, day_name STRING,
    is_weekend BOOLEAN, is_holiday BOOLEAN, week_of_year INT,
    created_at TIMESTAMP, updated_at TIMESTAMP
""")

# 4. DIM_PRODUCT
print("[4/8] Generando DIM_PRODUCT...")
df_dim_product = spark.sql("""
    SELECT stock_code, description, department,
        current_timestamp() AS created_at, current_timestamp() AS updated_at
    FROM silver_view GROUP BY stock_code, description, department
""")
register_table("dim_product", "s3://lakehouse-andres-2026-gold/dim_product", df_dim_product, """
    stock_code STRING, description STRING, department STRING,
    created_at TIMESTAMP, updated_at TIMESTAMP
""")

# 5. DIM_CUSTOMER
print("[5/8] Generando DIM_CUSTOMER...")
df_dim_customer = spark.sql("""
    SELECT customer_id, age,
        current_timestamp() AS created_at, current_timestamp() AS updated_at
    FROM silver_view GROUP BY customer_id, age
""")
register_table("dim_customer", "s3://lakehouse-andres-2026-gold/dim_customer", df_dim_customer, """
    customer_id INT, age INT,
    created_at TIMESTAMP, updated_at TIMESTAMP
""")

# 6. DIM_DEPARTMENT
print("[6/8] Generando DIM_DEPARTMENT...")
df_dim_dept = spark.sql("""
    SELECT department,
        current_timestamp() AS created_at, current_timestamp() AS updated_at
    FROM silver_view WHERE department IS NOT NULL AND department != ''
    GROUP BY department
""")
register_table("dim_department", "s3://lakehouse-andres-2026-gold/dim_department", df_dim_dept, """
    department STRING,
    created_at TIMESTAMP, updated_at TIMESTAMP
""")

# 7. DIM_COUNTRY
print("[7/8] Generando DIM_COUNTRY...")
df_dim_country = spark.sql("""
    SELECT country,
        current_timestamp() AS created_at, current_timestamp() AS updated_at
    FROM silver_view WHERE country IS NOT NULL AND country != ''
    GROUP BY country
""")
register_table("dim_country", "s3://lakehouse-andres-2026-gold/dim_country", df_dim_country, """
    country STRING,
    created_at TIMESTAMP, updated_at TIMESTAMP
""")

# 8. FACT_SALES (la joya de la corona)
print("[8/8] Generando FACT_SALES...")
df_fact = spark.sql("""
    SELECT
        invoice_id,
        stock_code,
        customer_id,
        CAST(year(invoice_date)*10000 + month(invoice_date)*100 + day(invoice_date) AS INT) AS date_key,
        department,
        country,
        quantity,
        price AS unit_price,
        CAST(quantity AS DOUBLE) * price AS revenue,
        current_timestamp() AS created_at,
        current_timestamp() AS updated_at
    FROM silver_view
    WHERE invoice_id IS NOT NULL AND invoice_id != ''
""")
register_table("fact_sales", "s3://lakehouse-andres-2026-gold/fact_sales", df_fact, """
    invoice_id STRING, stock_code STRING, customer_id INT, date_key INT,
    department STRING, country STRING, quantity INT,
    unit_price DOUBLE, revenue DOUBLE,
    created_at TIMESTAMP, updated_at TIMESTAMP
""")

# Verificación final
print("=" * 60)
print("VERIFICACIÓN DEL MODELO ESTRELLA")
print("=" * 60)
for t in ["dim_date","dim_product","dim_customer","dim_department","dim_country","fact_sales"]:
    c = spark.sql(f"SELECT COUNT(*) AS n FROM lakehouse_gold_db.{t}").collect()[0]["n"]
    print(f"  {t:15s} -> {c} filas")

print("=" * 60)
print("JOB GOLD completado exitosamente (Modelo Estrella Parquet)")
print("=" * 60)

job.commit()