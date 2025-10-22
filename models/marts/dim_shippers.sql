
WITH stg_shippers AS (
    
    SELECT
        *
    FROM {{ ref('stg_shippers') }}
),

final AS (
    SELECT
        -- (Surrogate Key)
        MD5(CAST(shipper_id AS VARCHAR)) AS shipper_key,

        --  Row Hash 
        MD5(
            CONCAT(
                CAST(shipper_id AS VARCHAR), '||',
                COALESCE(company_name, ''), '||',
                COALESCE(phone_digits, '')
            )
        ) AS row_hash,

       
        shipper_id,

        
        company_name AS shipper_company_name,
        phone_digits AS shipper_phone_digits

    FROM stg_shippers
    
   
    WHERE shipper_id IS NOT NULL
)

SELECT * FROM final