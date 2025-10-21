-- models/facts/fct_sales.sql

-- 1. جلب بيانات الحقائق من الـ Staging Models
WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
stg_order_details AS (
    SELECT * FROM {{ ref('stg_order_details') }}
),

-- 2. جلب المفاتيح البديلة من الـ Dimension Models
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
    -- 1. مفاتيح التحليل (استبدال الـ IDs بـ Keys)
    -- يتم إنشاء Hash لدمج OrderID و ProductID لإنشاء مفتاح فريد لسطر الطلب
    MD5(CAST(sod.order_id AS VARCHAR) || CAST(sod.product_id AS VARCHAR)) AS order_line_key,

    -- المفاتيح الأجنبية
    dc.customer_key,
    de.employee_key,
    dp.product_key,
    ds.shipper_key,
    
    -- 2. معلومات الطلب والتواريخ
    so.order_id,
    so.order_date,
    so.required_date,
    so.shipped_date,
    
    -- 3. المقاييس الأساسية (من stg_order_details)
    sod.unit_price,
    sod.quantity,
    sod.discount,
    
    -- المقاييس المشتقة: حساب الإيرادات النهائية (هذا هو أهم جزء)
    (sod.unit_price * sod.quantity) AS gross_revenue,
    (sod.unit_price * sod.quantity * sod.discount) AS discount_amount,
    -- الإيراد الصافي = الإجمالي * (1 - نسبة الخصم)
    (sod.unit_price * sod.quantity * (1 - sod.discount)) AS net_revenue,
    
    -- 4. معلومات الشحن الإضافية (من stg_orders)
    so.freight_cost,
    so.ship_name,
    so.ship_address,
    so.ship_city,
    so.ship_region,
    so.ship_postal_code,
    so.ship_country

FROM stg_order_details sod
-- الربط الأساسي: ربط تفاصيل الطلب برأس الطلب
INNER JOIN stg_orders so ON sod.order_id = so.order_id

-- الربط مع الأبعاد باستخدام المفاتيح الطبيعية (IDs) للحصول على المفاتيح البديلة (Keys)
LEFT JOIN dim_customers dc ON so.customer_id = dc.customer_id
LEFT JOIN dim_employees de ON so.employee_id = de.employee_id
LEFT JOIN dim_products dp ON sod.product_id = dp.product_id
LEFT JOIN dim_shippers ds ON so.ship_via_id = ds.shipper_id

-- ضمان أن لدينا بيانات صالحة للربط
WHERE so.order_id IS NOT NULL 
  AND sod.product_id IS NOT NULL