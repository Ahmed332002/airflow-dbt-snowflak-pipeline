

WITH source AS (
   
    SELECT
        *
    FROM {{ source('raw', 'employee_territory') }}
),

stg_clean AS (

    SELECT
       
        CAST(EmployeeID AS INT) AS employee_id,

       
        CAST(TRIM(TerritoryID) AS INT) AS territory_id
        
    FROM source
)

SELECT * FROM stg_clean