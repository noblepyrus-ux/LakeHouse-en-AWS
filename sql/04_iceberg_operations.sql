
-- ============================================================
-- APACHE ICEBERG OPERATIONS
-- Engine: Amazon Athena
-- Database: lakehouse_iceberg_db
-- ============================================================

-- 1. Create an Apache Iceberg table.
-- Run this statement only if the table does not exist.

CREATE TABLE IF NOT EXISTS lakehouse_iceberg_db.iceberg_sales_demo (
    invoice_id STRING,
    stock_code STRING,
    customer_id INT,
    quantity INT,
    unit_price DECIMAL(18,2),
    revenue DECIMAL(28,2)
)
LOCATION 's3://lakehouse-andres-2026-gold/iceberg_demo/sales/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);


-- 2. Load an initial sample of 100 records.
-- Execute only once on an empty table to avoid duplicates.

INSERT INTO lakehouse_iceberg_db.iceberg_sales_demo
SELECT
    CAST(invoice_id AS VARCHAR),
    CAST(stock_code AS VARCHAR),
    customer_id,
    quantity,
    CAST(unit_price AS DECIMAL(18,2)),
    CAST(revenue AS DECIMAL(28,2))
FROM lakehouse_gold_db.fact_sales
LIMIT 100;


-- 3. Check the current number of visible records.

SELECT COUNT(*) AS total_records
FROM lakehouse_iceberg_db.iceberg_sales_demo;


-- 4. Demonstrate MERGE INTO with a test record.

MERGE INTO lakehouse_iceberg_db.iceberg_sales_demo AS target
USING (
    SELECT
        'ICEBERG-DEMO-001' AS invoice_id,
        'DEMO-PRODUCT-001' AS stock_code,
        999999 AS customer_id,
        2 AS quantity,
        CAST(150.00 AS DECIMAL(18,2)) AS unit_price,
        CAST(300.00 AS DECIMAL(28,2)) AS revenue
) AS source
ON target.invoice_id = source.invoice_id
WHEN MATCHED THEN
    UPDATE SET
        quantity = source.quantity,
        unit_price = source.unit_price,
        revenue = source.revenue
WHEN NOT MATCHED THEN
    INSERT (
        invoice_id,
        stock_code,
        customer_id,
        quantity,
        unit_price,
        revenue
    )
    VALUES (
        source.invoice_id,
        source.stock_code,
        source.customer_id,
        source.quantity,
        source.unit_price,
        source.revenue
    );


-- 5. Delete the test record.

DELETE FROM lakehouse_iceberg_db.iceberg_sales_demo
WHERE invoice_id = 'ICEBERG-DEMO-001';


-- 6. Inspect the Iceberg snapshot history.

SELECT *
FROM "lakehouse_iceberg_db"."iceberg_sales_demo$snapshots"
ORDER BY committed_at DESC;


-- 7. Query historical data before the DELETE operation.
-- This snapshot previously contained 101 visible records.
SELECT COUNT(*) AS historical_records
FROM lakehouse_iceberg_db.iceberg_sales_demo
FOR VERSION AS OF 6495237613004130167;


-- 8. Compare with the current table.
SELECT COUNT(*) AS current_records
FROM lakehouse_iceberg_db.iceberg_sales_demo;