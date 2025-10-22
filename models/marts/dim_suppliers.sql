
WITH stg_suppliers AS (
    
    SELECT
        *
    FROM {{ ref('stg_suppliers') }}
),

final AS (
    SELECT
       
        MD5(CAST(supplier_id AS VARCHAR)) AS supplier_key,
        
        
        MD5(
            CONCAT(
                CAST(supplier_id AS VARCHAR), '||', 
                COALESCE(company_name, ''), '||', 
                COALESCE(contact_name, ''), '||', 
                COALESCE(country, ''), '||', 
                COALESCE(phone, '')
            )
        ) AS row_hash,

      
        supplier_id,

       
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        region,
        postal_code,
        country,
        phone,
        fax,
        homepage

    FROM stg_suppliers
    
   
    WHERE supplier_id IS NOT NULL 
)

SELECT * FROM final