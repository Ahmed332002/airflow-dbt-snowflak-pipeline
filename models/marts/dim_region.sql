-- models/dimensions/dim_regions.sql

WITH stg_regions AS (
    -- 1. جلب البيانات النظيفة من الـ Staging model
    SELECT
        *
    FROM {{ ref('stg_region') }}
),

final AS (
    SELECT
        -- 1. المفتاح البديل (Surrogate Key)
        MD5(CAST(region_id AS VARCHAR)) AS region_key,
        
        -- 2. Row Hash (لتتبع التغييرات في الوصف إذا لزم الأمر)
        MD5(
            CONCAT(
                CAST(region_id AS VARCHAR), '||', 
                COALESCE(region_description_raw, '')
            )
        ) AS row_hash,

        -- 3. المفتاح الطبيعي
        region_id,
        
        -- 4. السمات النهائية
        region_description_raw AS region_description
        
    FROM stg_regions
    
    -- التأكد من أن المنطقة لديها هوية صالحة
    WHERE region_id IS NOT NULL 
)

SELECT * FROM final