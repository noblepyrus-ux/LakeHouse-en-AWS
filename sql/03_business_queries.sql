
-- ============================================================
-- BUSINESS ANALYTICS QUERIES
-- Database: lakehouse_gold_db
-- Engine: Amazon Athena
-- Source: Gold Layer (Star Schema)
-- ============================================================

-- 1. General sales KPIs
-- Calculate total transactions, invoices, units, revenue,
-- and average revenue per transaction.

SELECT
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT invoice_id) AS total_invoices,
    SUM(quantity) AS total_units,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_revenue
FROM lakehouse_gold_db.fact_sales;

-- 2. Revenue by country
-- Identify countries generating the highest revenue.

SELECT
    country,
    COUNT(*) AS total_transactions,
    SUM(revenue) AS total_revenue
FROM lakehouse_gold_db.fact_sales
GROUP BY country
ORDER BY total_revenue DESC;

-- 3. Top 10 products by revenue
-- Join sales facts with the product dimension.

SELECT
    p.stock_code,
    p.description,
    SUM(f.quantity) AS total_units,
    SUM(f.revenue) AS total_revenue
FROM lakehouse_gold_db.fact_sales f
JOIN lakehouse_gold_db.dim_product p
    ON f.stock_code = p.stock_code
GROUP BY
    p.stock_code,
    p.description
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. Revenue by department
-- Analyze revenue contribution by business department.

SELECT
    department,
    COUNT(*) AS total_transactions,
    SUM(revenue) AS total_revenue
FROM lakehouse_gold_db.fact_sales
GROUP BY department
ORDER BY total_revenue DESC;

-- 5. Monthly sales trend
-- Analyze revenue over time using the date dimension.

SELECT
    d.year,
    d.month,
    SUM(f.revenue) AS total_revenue
FROM lakehouse_gold_db.fact_sales f
JOIN lakehouse_gold_db.dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month;