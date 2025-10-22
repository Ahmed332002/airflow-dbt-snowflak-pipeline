
WITH stg_customers AS (
   
    SELECT
        *
    FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        -- (Surrogate Key)
        MD5(customer_id) AS customer_key,
        
        -- Row Hash 
        MD5(
            CONCAT(
                customer_id, '||',
                COALESCE(company_name, ''), '||',
                COALESCE(contact_name, ''), '||',
                COALESCE(contact_title, ''), '||',
                COALESCE(address, ''), '||',
                COALESCE(city, ''), '||',
                COALESCE(country, ''), '||',
                COALESCE(postal_code, ''), '||',
                COALESCE(region, ''), '||',
                COALESCE(phone, ''), '||',
                COALESCE(fax, '')
            )
        ) AS row_hash,

       
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

    FROM stg_customers
)

SELECT * FROM final