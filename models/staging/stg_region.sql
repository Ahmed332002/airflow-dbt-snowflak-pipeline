
WITH source AS (
   
    SELECT
        *
    FROM {{ source('raw', 'region') }}
),

stg_clean AS (
    
    SELECT
        CAST(RegionID AS INT) AS region_id,

    
        INITCAP(REGEXP_REPLACE(TRIM(RegionDescription), '\s+', ' ')) AS region_description_raw

    FROM source
)


SELECT * FROM stg_clean