WITH fct_sales AS (
    -- 1. جلب الحقائق الرئيسية
    SELECT
        order_date,
        customer_key,
        employee_key,
        order_id,
        net_revenue
    FROM {{ ref('fct_sales') }}
),

-- 2. حساب عدد الطلبات لكل موظف لكل عميل (علشان نجيب الموظف الأكثر ارتباطاً)
customer_employee_ranked AS (
    SELECT
        customer_key,
        employee_key,
        COUNT(order_id) AS orders_by_employee,
        ROW_NUMBER() OVER (
            PARTITION BY customer_key
            ORDER BY COUNT(order_id) DESC
        ) AS rnk
    FROM fct_sales
    GROUP BY customer_key, employee_key
),

-- 3. تجميع المقاييس العامة لكل عميل
customer_aggregates AS (
    SELECT
        customer_key,
        SUM(net_revenue) AS customer_lifetime_value,
        COUNT(DISTINCT order_id) AS total_orders_count,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS latest_order_date
    FROM fct_sales
    GROUP BY customer_key
),

-- 4. الدمج بين الجداول للحصول على النتائج النهائية
final AS (
    SELECT
        ca.customer_key,
        ce.employee_key AS main_employee_key,

        ca.first_order_date,
        ca.latest_order_date,

        ca.customer_lifetime_value,
        ca.total_orders_count,

        -- متوسط قيمة الطلب
        (ca.customer_lifetime_value / NULLIF(ca.total_orders_count, 0)) AS average_order_value,

        -- حساب المدة الزمنية كعميل (عدد الأيام)
        DATEDIFF('day', ca.first_order_date, CURRENT_DATE()) AS days_as_customer

    FROM customer_aggregates ca
    LEFT JOIN customer_employee_ranked ce
        ON ca.customer_key = ce.customer_key
       AND ce.rnk = 1
)

SELECT * FROM final
