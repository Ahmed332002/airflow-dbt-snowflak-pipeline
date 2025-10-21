
WITH source AS (
   
    SELECT
        *
    FROM {{ source('raw', 'territories') }}
),

stg_clean AS (
   
    SELECT
       
        TRIM(TerritoryID) AS territory_id, 
        CAST(RegionID AS INT) AS region_id,

    
        INITCAP(REGEXP_REPLACE(TRIM(TerritoryDescription), '\s+', ' ')) AS territory_description

    FROM source
)


SELECT * FROM stg_clean