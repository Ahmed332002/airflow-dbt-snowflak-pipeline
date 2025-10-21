
WITH source AS (
  
    SELECT
        *
    FROM {{ source('raw', 'orders') }}
),

stg_clean AS (
 
    SELECT
 
        CAST(OrderID AS INT) AS order_id,
        TRIM(CustomerID) AS customer_id, 
        CAST(EmployeeID AS INT) AS employee_id,
         

        DATE_TRUNC('DAY', OrderDate) AS order_date,
        DATE_TRUNC('DAY', RequiredDate) AS required_date,
        
      
        DATE_TRUNC('DAY', TRY_TO_TIMESTAMP(ShippedDate, 'MM/DD/YYYY HH24:MI:SS')) AS shipped_date,

        CAST(ShipVia AS INT) AS ship_via_id,
        
       
        CAST(Freight AS NUMERIC(10, 4)) AS freight_cost,
        INITCAP(REGEXP_REPLACE(TRIM(ShipName), '\s+', ' ')) AS ship_name,
        REGEXP_REPLACE(TRIM(ShipAddress), '\s+', ' ') AS ship_address,
        INITCAP(TRIM(ShipCity)) AS ship_city,
        
    
        NULLIF(UPPER(TRIM(ShipRegion)), 'NULL') AS ship_region,
        
       
        {{ clean_postal_code('ShipPostalCode') }} AS ship_postal_code,

        {{ standardize_country('ShipCountry') }} AS ship_country
        
      

    FROM source
)


SELECT * FROM stg_clean