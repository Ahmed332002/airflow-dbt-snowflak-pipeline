
WITH stg_regions AS (
   
    SELECT
        *
    FROM {{ ref('stg_region') }}
),

final AS (
    SELECT
        -- (Surrogate Key)
        MD5(CAST(region_id AS VARCHAR)) AS region_key,
        
        -- 2. Row Hash 
        MD5(
            CONCAT(
                CAST(region_id AS VARCHAR), '||', 
                COALESCE(region_description_raw, '')
            )
        ) AS row_hash,

        
        region_id,
        
       
        region_description_raw AS region_description
        
    FROM stg_regions
    
   
    WHERE region_id IS NOT NULL 
)

SELECT * FROM final