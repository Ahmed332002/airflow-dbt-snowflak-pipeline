
WITH stg_products AS (
   
    SELECT
        *
    FROM {{ ref('stg_products') }}
),

dim_suppliers AS (
   
    SELECT
        supplier_key,
        supplier_id
    FROM {{ ref('dim_suppliers') }}
),

dim_categories AS (
 
    SELECT
        category_key,
        category_id
    FROM {{ ref('dim_category') }}
)

SELECT
   
    MD5(CAST(sp.product_id AS VARCHAR)) AS product_key,
    
   
    ds.supplier_key,
    dc.category_key,
    
    
    MD5(
        CONCAT(
            CAST(sp.product_id AS VARCHAR), '||', 
            COALESCE(sp.product_name, ''), '||', 
            CAST(sp.unit_price AS VARCHAR), '||',
            CAST(sp.discontinued AS VARCHAR)
        )
    ) AS row_hash,
    
    
    sp.product_id,
    
  
    sp.product_name,
    sp.quantity_per_unit,
    sp.unit_price,
    sp.units_in_stock,
    sp.units_on_order,
    sp.reorder_level,
    
    
    CASE 
        WHEN sp.discontinued = 1 THEN TRUE 
        ELSE FALSE 
    END AS is_discontinued
    
FROM stg_products sp

LEFT JOIN dim_suppliers ds ON sp.supplier_id = ds.supplier_id
LEFT JOIN dim_categories dc ON sp.category_id = dc.category_id


WHERE sp.product_id IS NOT NULL