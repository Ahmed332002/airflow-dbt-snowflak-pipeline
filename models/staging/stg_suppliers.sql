
WITH source AS (
   
    SELECT
        *
    FROM {{ source('raw', 'suppliers') }}
),

stg_clean AS (
    
    SELECT
      
        TRY_CAST(TRIM(SupplierID) AS INT) AS supplier_id,

      
        {{ standardize_name("CompanyName") }} AS company_name,
        {{ standardize_name("ContactName") }} AS contact_name,
        {{ standardize_name("ContactTitle") }} AS contact_title,
        
       
        REGEXP_REPLACE(NULLIF(TRIM(Address), ''), '\s+', ' ') AS address,

        {{ standardize_name("City") }} AS city,
        
       
        NULLIF(UPPER(TRIM(Region)), 'NULL') AS region, 
        
       
        {{ clean_postal_code("PostalCode") }} AS postal_code,
        
       
        {{ standardize_country("Country") }} AS country,

        
        {{ clean_phone("Phone") }} AS phone,
        {{ clean_phone("Fax") }} AS fax,


        NULLIF(TRIM(HomePage), '') AS homepage

    FROM source
)




SELECT * FROM stg_clean