
WITH source_data AS (
    SELECT
        CategoryID,
        TRIM(CategoryName) AS CategoryName,
        TRIM(Description) AS Description
    FROM {{ source('raw', 'categories') }}
),

cleaned_data AS (
    SELECT
        CategoryID,
        
        CASE 
            WHEN CategoryName IS NULL OR CategoryName = '' THEN 'Unknown'
            ELSE CategoryName
        END AS CategoryName,
        
        
        CASE 
            WHEN Description IS NULL OR Description = '' THEN 'No description'
            ELSE Description
        END AS Description
    FROM source_data
)

SELECT *
FROM cleaned_data
