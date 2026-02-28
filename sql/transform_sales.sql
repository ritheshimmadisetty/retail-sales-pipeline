-- ================================================
-- TRANSFORMATION: raw.sales -> transformed.sales_summary
-- Purpose: Summarize sales data by date, category and city
-- ================================================

-- First clear any existing data in the summary table
-- We do this so we can run this transformation multiple times
-- without creating duplicate records
TRUNCATE TABLE retail_db.transformed.sales_summary;

-- Now insert the summarized data
INSERT INTO retail_db.transformed.sales_summary (
    order_date,
    category,
    city,
    total_orders,
    total_revenue,
    avg_order_value
)
SELECT
    order_date,
    category,
    city,
    COUNT(order_id)          AS total_orders,
    SUM(total_amount)        AS total_revenue,
    AVG(total_amount)        AS avg_order_value
FROM retail_db.raw.sales
WHERE status = 'Completed'
GROUP BY order_date, category, city
ORDER BY order_date, category, city;