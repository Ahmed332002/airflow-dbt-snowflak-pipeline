
WITH stg_territories AS (
    
    SELECT
        *
    FROM {{ ref('stg_territories') }}
),

dim_regions AS (
   
    SELECT
        region_key,
        region_id
    FROM {{ ref('dim_region') }}
)

SELECT
   
    MD5(st.territory_id) AS territory_key,
    
   
    dr.region_key,
    
   
    MD5(
        CONCAT(
            st.territory_id, '||', 
            COALESCE(st.territory_description, ''), '||',
            CAST(st.region_id AS VARCHAR)
        )
    ) AS row_hash,
    
    
    st.territory_id,
    
    
    st.territory_description
    
FROM stg_territories st

LEFT JOIN dim_regions dr ON st.region_id = dr.region_id


WHERE st.territory_id IS NOT NULL