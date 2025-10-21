-- models/dimensions/dim_suppliers.sql

WITH stg_suppliers AS (
    -- 1. جلب البيانات النظيفة والمُنقاة من نموذج Staging
    SELECT
        *
    FROM {{ ref('stg_suppliers') }}
),

final AS (
    SELECT
        -- 1. المفتاح البديل (Surrogate Key)
        -- يُستخدم لربط المنتجات بهذا البعد
        MD5(CAST(supplier_id AS VARCHAR)) AS supplier_key,
        
        -- 2. Row Hash (لتتبع التغييرات في سمات المورد، لـ SCD Type 2)
        MD5(
            CONCAT(
                CAST(supplier_id AS VARCHAR), '||', 
                COALESCE(company_name, ''), '||', 
                COALESCE(contact_name, ''), '||', 
                COALESCE(country, ''), '||', 
                COALESCE(phone, '')
            )
        ) AS row_hash,

        -- 3. المفتاح الطبيعي
        supplier_id,

        -- 4. السمات الوصفية
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        region,
        postal_code,
        country,
        phone,
        fax,
        homepage

    FROM stg_suppliers
    
    -- **الخطوة الحاسمة:** إزالة السجلات التي ليس لها هوية مورد صالحة.
    -- هذا يتخلص من السجل الذي كان يحمل 'null' في المصدر.
    WHERE supplier_id IS NOT NULL 
)

SELECT * FROM final