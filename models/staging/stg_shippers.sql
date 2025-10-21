
WITH source AS (
   
    SELECT
        *
    FROM {{ source('raw', 'shippers') }}
),

stg_clean AS (
  
    SELECT
   
        CAST(ShipperID AS INT) AS shipper_id,

       
        INITCAP(REGEXP_REPLACE(TRIM(CompanyName), '\s+', ' ')) AS company_name,
        
        
        
        {{ clean_phone("Phone") }} AS phone_digits
        

    FROM source
)


SELECT * FROM stg_clean