-- models/dimensions/dim_products.sql

WITH stg_products AS (
    -- 1. جلب البيانات النظيفة من الـ Staging model
    SELECT
        *
    FROM {{ ref('stg_products') }}
),

dim_suppliers AS (
    -- 2. جلب المفاتيح البديلة للموردين
    SELECT
        supplier_key,
        supplier_id
    FROM {{ ref('dim_suppliers') }}
),

dim_categories AS (
    -- 3. جلب المفاتيح البديلة للفئات
    SELECT
        category_key,
        category_id
    FROM {{ ref('dim_category') }}
)

SELECT
    -- 1. المفتاح البديل للمنتج
    MD5(CAST(sp.product_id AS VARCHAR)) AS product_key,
    
    -- 2. المفاتيح الأجنبية (ربط الأبعاد)
    ds.supplier_key,
    dc.category_key,
    
    -- 3. Row Hash (لتتبع التغييرات في سمات المنتج)
    MD5(
        CONCAT(
            CAST(sp.product_id AS VARCHAR), '||', 
            COALESCE(sp.product_name, ''), '||', 
            CAST(sp.unit_price AS VARCHAR), '||',
            CAST(sp.discontinued AS VARCHAR)
        )
    ) AS row_hash,
    
    -- 4. المفاتيح الطبيعية
    sp.product_id,
    
    -- 5. السمات الوصفية والكميات
    sp.product_name,
    sp.quantity_per_unit,
    sp.unit_price,
    sp.units_in_stock,
    sp.units_on_order,
    sp.reorder_level,
    
    -- تحويل 0/1 إلى قيمة منطقية (Boolean/Flag) واضحة
    CASE 
        WHEN sp.discontinued = 1 THEN TRUE 
        ELSE FALSE 
    END AS is_discontinued
    
FROM stg_products sp
-- الربط على المفاتيح الطبيعية (IDs) للحصول على المفاتيح البديلة (Keys)
LEFT JOIN dim_suppliers ds ON sp.supplier_id = ds.supplier_id
LEFT JOIN dim_categories dc ON sp.category_id = dc.category_id

-- التأكد من أن المنتج لديه هوية صالحة
WHERE sp.product_id IS NOT NULL