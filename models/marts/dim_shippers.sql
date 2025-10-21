-- models/dimensions/dim_shippers.sql

WITH stg_shippers AS (
    -- 1. جلب البيانات النظيفة من نموذج Staging
    SELECT
        *
    FROM {{ ref('stg_shippers') }}
),

final AS (
    SELECT
        -- 1. المفتاح البديل (Surrogate Key)
        MD5(CAST(shipper_id AS VARCHAR)) AS shipper_key,

        -- 2. Row Hash (لتتبع التغييرات في سمات شركة الشحن)
        MD5(
            CONCAT(
                CAST(shipper_id AS VARCHAR), '||',
                COALESCE(company_name, ''), '||',
                COALESCE(phone_digits, '')
            )
        ) AS row_hash,

        -- 3. المفتاح الطبيعي
        shipper_id,

        -- 4. السمات الوصفية النهائية
        company_name AS shipper_company_name,
        phone_digits AS shipper_phone_digits

    FROM stg_shippers
    
    -- التأكد من أن شركة الشحن لديها هوية صالحة
    WHERE shipper_id IS NOT NULL
)

SELECT * FROM final