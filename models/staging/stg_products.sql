
WITH source AS (

    SELECT
        *
    FROM {{ source('raw', 'products') }}
),

stg_clean AS (
   
    SELECT
      
        CAST(ProductID AS INT) AS product_id,
        CAST(SupplierID AS INT) AS supplier_id,
        CAST(CategoryID AS INT) AS category_id,

     
        INITCAP(REGEXP_REPLACE(TRIM(ProductName), '\s+', ' ')) AS product_name,
        REGEXP_REPLACE(TRIM(QuantityPerUnit), '\s+', ' ') AS quantity_per_unit,

    

        CAST(UnitPrice AS NUMERIC(10, 4)) AS unit_price,
        
        
        CAST(UnitsInStock AS INT) AS units_in_stock,
        CAST(UnitsOnOrder AS INT) AS units_on_order,
        CAST(ReorderLevel AS INT) AS reorder_level,
        
      
        CAST(Discontinued AS INT) AS discontinued
        
    FROM source
)

SELECT * FROM stg_clean