
WITH source_dates AS (
    
    SELECT DISTINCT order_date AS date_day FROM {{ ref('stg_orders') }} WHERE order_date IS NOT NULL
    UNION
    SELECT DISTINCT shipped_date AS date_day FROM {{ ref('stg_orders') }} WHERE shipped_date IS NOT NULL
    UNION
    SELECT DISTINCT required_date AS date_day FROM {{ ref('stg_orders') }} WHERE required_date IS NOT NULL
),

dim_date_raw AS (
    SELECT
        date_day,
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,

        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        
        DAYNAME(date_day) AS weekday,
        
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMM')) AS year_month_key

    FROM source_dates
    ORDER BY date_day
)

SELECT 
    date_key,
    date_day AS date,
    year,
    month,
    day,
    quarter,
    weekday,
    year_month_key
FROM dim_date_raw