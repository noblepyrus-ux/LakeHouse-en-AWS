-- ============================================================
-- FASE 06: CAPA GOLD (Modelo Estrella Puro)
-- 1 Tabla de Hechos + 5 Dimensiones (Apache Iceberg)
-- Ejecutar cada bloque CREATE TABLE por separado en Athena/CLI.
-- ============================================================

-- 1. Schema Gold
CREATE SCHEMA lakehouse_gold_db
WITH DBPROPERTIES ('location' = 's3://lakehouse-andres-2026-gold/');

-- 2. DIM_DATE (Línea de tiempo rica, generada por rango)
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
FROM (SELECT CAST(t AS DATE) AS d FROM UNNEST(SEQUENCE(DATE '2024-01-01', DATE '2027-12-31', INTERVAL '1' DAY)) AS x(t));

-- 3. DIM_PRODUCT
CREATE TABLE lakehouse_gold_db.dim_product
WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_product/', is_external = false) AS
SELECT stock_code, description, department,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
FROM lakehouse_silver_db.sales_silver GROUP BY stock_code, description, department;

-- 4. DIM_CUSTOMER
CREATE TABLE lakehouse_gold_db.dim_customer
WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_customer/', is_external = false) AS
SELECT customer_id, age,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
FROM lakehouse_silver_db.sales_silver GROUP BY customer_id, age;

-- 5. DIM_DEPARTMENT
CREATE TABLE lakehouse_gold_db.dim_department
WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_department/', is_external = false) AS
SELECT department,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
FROM lakehouse_silver_db.sales_silver WHERE department IS NOT NULL AND department != '' GROUP BY department;

-- 6. DIM_COUNTRY
CREATE TABLE lakehouse_gold_db.dim_country
WITH (table_type = 'ICEBERG', format = 'PARQUET', location = 's3://lakehouse-andres-2026-gold/dim_country/', is_external = false) AS
SELECT country,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_at
FROM lakehouse_silver_db.sales_silver WHERE country IS NOT NULL AND country != '' GROUP BY country;

-- 7. FACT_SALES (Tabla de Hechos con 5 FKs directas)
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
WHERE invoice_id IS NOT NULL AND invoice_id != '';