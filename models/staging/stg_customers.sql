
WITH source AS (
    -- Step 1: Extract raw data from the source
    SELECT
        *
    FROM {{ source('raw', 'customers') }}
),

stg_clean AS (
    -- Step 2: Perform data cleaning and standardization for all columns
    SELECT
        -- Clean and trim the customer ID
        TRIM(CustomerID) AS customer_id,

        -- Clean and normalize company and contact information
        
        {{ standardize_name("CompanyName") }} AS company_name,
        {{ standardize_name("ContactName") }} AS contact_name,
        {{ standardize_name("ContactTitle") }} AS contact_title,

        -- Clean and normalize address and city fields
        REGEXP_REPLACE(NULLIF(TRIM(Address), ''), '\s+', ' ') AS address,
        {{ standardize_name("City") }} AS city,

        
        {{ standardize_country("Country") }} AS country,

        -- Clean postal code and region fields
        {{ clean_postal_code("PostalCode") }} AS postal_code,
        NULLIF(UPPER(TRIM(Region)), 'NULL') AS region,
        
        

        -- Remove all non-numeric characters from phone and fax numbers
        
        {{ clean_phone("Phone") }} AS phone,
        {{ clean_phone("Fax") }} AS fax,


    FROM source
),

deduplicated AS (
    -- Step 3: Remove duplicates based on customer_id
    SELECT
        *
    FROM (
        SELECT
            stg_clean.*,
            -- Rank rows for each customer_id by the most recent load timestamp
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id DESC) AS rn
        FROM stg_clean
    )
    -- Keep only the most recent record per customer_id
    WHERE rn = 1
)

-- Final Step: Output the cleaned and deduplicated data
SELECT 
    customer_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    country,
    postal_code,
    region,
    phone,
    fax
FROM deduplicated
