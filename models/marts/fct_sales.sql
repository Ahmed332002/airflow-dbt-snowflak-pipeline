
WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
stg_order_details AS (
    SELECT * FROM {{ ref('stg_order_details') }}
),


dim_customers AS ( 
    SELECT customer_key, customer_id FROM {{ ref('dim_customers') }} 
),
dim_employees AS ( 
    SELECT employee_key, employee_id FROM {{ ref('dim_employees') }} 
),
dim_products AS ( 
    SELECT product_key, product_id FROM {{ ref('dim_products') }} 
),
dim_shippers AS ( 
    SELECT shipper_key, shipper_id FROM {{ ref('dim_shippers') }} 
)

SELECT
   
    MD5(CAST(sod.order_id AS VARCHAR) || CAST(sod.product_id AS VARCHAR)) AS order_line_key,

    
    dc.customer_key,
    de.employee_key,
    dp.product_key,
    ds.shipper_key,
    
   
    so.order_id,
    so.order_date,
    so.required_date,
    so.shipped_date,
    
   
    sod.unit_price,
    sod.quantity,
    sod.discount,
    
    
    (sod.unit_price * sod.quantity) AS gross_revenue,
    (sod.unit_price * sod.quantity * sod.discount) AS discount_amount,
    
    (sod.unit_price * sod.quantity * (1 - sod.discount)) AS net_revenue,
    
    
    so.freight_cost,
    so.ship_name,
    so.ship_address,
    so.ship_city,
    so.ship_region,
    so.ship_postal_code,
    so.ship_country

FROM stg_order_details sod

INNER JOIN stg_orders so ON sod.order_id = so.order_id


LEFT JOIN dim_customers dc ON so.customer_id = dc.customer_id
LEFT JOIN dim_employees de ON so.employee_id = de.employee_id
LEFT JOIN dim_products dp ON sod.product_id = dp.product_id
LEFT JOIN dim_shippers ds ON so.ship_via_id = ds.shipper_id


WHERE so.order_id IS NOT NULL 
  AND sod.product_id IS NOT NULL