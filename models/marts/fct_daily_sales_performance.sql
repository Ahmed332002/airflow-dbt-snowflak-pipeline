-- models/facts/fct_daily_sales_performance.sql

WITH fct_sales AS (
    -- 1. جلب حقائق المبيعات التفصيلية التي أنشأناها مسبقاً
    SELECT
        order_date,
        customer_key,
        employee_key,
        order_id,
        net_revenue,
        gross_revenue,
        quantity
    FROM {{ ref('fct_sales') }}
)

SELECT
    -- المفاتيح
    fs.order_date AS date_key,
    fs.customer_key,
    fs.employee_key,
    
    -- المقاييس المُجمّعة
    SUM(fs.net_revenue) AS total_net_revenue,
    SUM(fs.gross_revenue) AS total_gross_revenue,
    SUM(fs.quantity) AS total_quantity_sold,
    
    -- عدد الطلبات (يتم حسابه عبر تمييز الطلبات الفريدة)
    COUNT(DISTINCT fs.order_id) AS total_orders_placed
    
FROM fct_sales fs
GROUP BY 1, 2, 3