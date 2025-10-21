

WITH source AS (
   
    SELECT
        *
    FROM {{ source('raw', 'employees') }}
),

stg_clean AS (
   
    SELECT
     
        CAST(EmployeeID AS INT) AS employee_id,

       
        INITCAP(TRIM(LastName)) AS last_name,
        INITCAP(TRIM(FirstName)) AS first_name,
        INITCAP(TRIM(Title)) AS job_title,
        INITCAP(TRIM(TitleOfCourtesy)) AS title_of_courtesy,
        
      

        TRY_TO_DATE(REGEXP_REPLACE(TRIM(BirthDate), '\\s+', ' '), 'MM/DD/YYYY HH24:MI') AS birth_date,
        TRY_TO_DATE(REGEXP_REPLACE(TRIM(HireDate), '\\s+', ' '), 'MM/DD/YYYY HH24:MI') AS hire_date,



    
        REGEXP_REPLACE(TRIM(Address), '\s+', ' ') AS address,
        INITCAP(TRIM(City)) AS city,
        NULLIF(UPPER(TRIM(Region)), 'NULL') AS region, 
        
        
        UPPER(TRIM(Country)) AS country,

      
        {{ clean_phone("HomePhone") }} AS home_phone,
       
      
        CAST(Salary AS NUMERIC(10, 2)) AS salary
        

    FROM source
)



SELECT * FROM stg_clean