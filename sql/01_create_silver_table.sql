-- ==========================================
-- FASE 05: CREAR CAPA SILVER (ICEBERG)
-- Transformación: Bronze → Silver
-- Limpieza: Eliminar duplicados, corregir tipos, auditoría
-- ==========================================

-- 1. Crear la base de datos para la capa Silver
CREATE DATABASE IF NOT EXISTS lakehouse_silver_db;

-- 2. Crear la tabla Silver en formato Apache Iceberg con datos limpios
CREATE TABLE lakehouse_silver_db.sales_silver
WITH (
    table_type = 'ICEBERG',
    format = 'PARQUET',
    location = 's3://lakehouse-andres-2026-silver/sales_silver/'
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
    CURRENT_TIMESTAMP AS processed_at
FROM (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY invoice_id, stock_code, invoice_date 
            ORDER BY invoice_date DESC
        ) as rn
    FROM lakehouse_bronze_db.lakehouse_andres_2026_landing
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND quantity != 0
)
WHERE rn = 1;