WITH fct_sales AS (
   
    SELECT
        order_date,
        customer_key,
        employee_key,
        order_id,
        net_revenue
    FROM {{ ref('fct_sales') }}
),


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


final AS (
    SELECT
        ca.customer_key,
        ce.employee_key AS main_employee_key,

        ca.first_order_date,
        ca.latest_order_date,

        ca.customer_lifetime_value,
        ca.total_orders_count,

        
        (ca.customer_lifetime_value / NULLIF(ca.total_orders_count, 0)) AS average_order_value,

        
        DATEDIFF('day', ca.first_order_date, CURRENT_DATE()) AS days_as_customer

    FROM customer_aggregates ca
    LEFT JOIN customer_employee_ranked ce
        ON ca.customer_key = ce.customer_key
       AND ce.rnk = 1
)

SELECT * FROM final
