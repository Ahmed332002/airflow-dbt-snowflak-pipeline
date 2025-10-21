
WITH stg_categories AS (

    SELECT
        *
    FROM {{ ref('stg_categories') }}
),

final AS (
    SELECT
      
        MD5(CAST(CategoryID AS VARCHAR)) AS category_key,
        
     
        MD5(
            CAST(CategoryID AS VARCHAR) || '||' ||
            COALESCE(CategoryName, '') || '||' ||
            COALESCE(Description, '')
        ) AS row_hash,

      
        CAST(CategoryID AS INT) AS category_id,
        
  
        CategoryName AS category_name,
        Description AS category_description
        
    FROM stg_categories
)

SELECT * FROM final