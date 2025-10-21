-- models/dimensions/dim_territories.sql

WITH stg_territories AS (
    -- 1. جلب البيانات النظيفة من الـ Staging model
    SELECT
        *
    FROM {{ ref('stg_territories') }}
),

dim_regions AS (
    -- 2. جلب المفاتيح البديلة للمناطق الرئيسية (Regions)
    SELECT
        region_key,
        region_id
    FROM {{ ref('dim_region') }}
)

SELECT
    -- 1. المفتاح البديل للكيان (Surrogate Key)
    -- نستخدم TerritoryID كنص (VARCHAR) لضمان عدم فقدان الأصفار البادئة.
    MD5(st.territory_id) AS territory_key,
    
    -- 2. المفتاح الأجنبي (Foreign Key)
    -- نربط على المفتاح الطبيعي (region_id) للحصول على المفتاح البديل (region_key)
    dr.region_key,
    
    -- 3. Row Hash (لتتبع التغييرات في سمات المنطقة)
    MD5(
        CONCAT(
            st.territory_id, '||', 
            COALESCE(st.territory_description, ''), '||',
            CAST(st.region_id AS VARCHAR)
        )
    ) AS row_hash,
    
    -- 4. المفتاح الطبيعي
    st.territory_id,
    
    -- 5. السمات النهائية
    st.territory_description
    
FROM stg_territories st
-- الربط مع جدول الأبعاد dim_regions
LEFT JOIN dim_regions dr ON st.region_id = dr.region_id

-- التأكد من أن المنطقة لديها هوية صالحة
WHERE st.territory_id IS NOT NULL