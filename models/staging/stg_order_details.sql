

WITH source AS (
   
    SELECT
        *
    FROM {{ source('raw', 'order_details') }}
),

stg_clean AS (

    SELECT

        CAST(OrderID AS INT) AS order_id,
        CAST(ProductID AS INT) AS product_id,
        CAST(UnitPrice AS NUMERIC(10, 4)) AS unit_price,
        CAST(Quantity AS INT) AS quantity,
        CAST(Discount AS NUMERIC(5, 4)) AS discount
        
    FROM source
)


SELECT * FROM stg_clean