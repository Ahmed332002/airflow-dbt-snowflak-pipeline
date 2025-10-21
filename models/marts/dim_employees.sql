

WITH stg_employees AS (
    
    SELECT * FROM {{ ref('stg_employees') }}
),

stg_employee_territory AS (
    --  جلب بيانات ربط الموظفين بالمناطق
    SELECT * FROM {{ ref('stg_employee_territory') }}
    WHERE employee_id IS NOT NULL AND territory_id IS NOT NULL
),

grouped_territories AS (
    --  تجميع المناطق لكل موظف
    SELECT
        employee_id,
        LISTAGG(territory_id, ', ') WITHIN GROUP (ORDER BY territory_id) AS territories_list,
        COUNT(territory_id) AS total_territories_covered
    FROM stg_employee_territory
    GROUP BY 1
),

final AS (
    SELECT
        -- 🔹 Surrogate Key
        MD5(CAST(se.employee_id AS VARCHAR)) AS employee_key,

        -- 🔹 Row Hash (لتتبع التغييرات)
        MD5(
            CONCAT(
                se.employee_id, '||',
                COALESCE(se.last_name, ''), '||',
                COALESCE(se.job_title, ''), '||',
                COALESCE(se.country, ''), '||',
                COALESCE(gt.total_territories_covered, 0)
            )
        ) AS row_hash,

        -- 🔹 Natural Key
        se.employee_id,

        -- 🔹 السمات
        se.first_name,
        se.last_name,
        
        se.title_of_courtesy,
        se.job_title,

        -- 🔹 التواريخ
        se.birth_date,
        se.hire_date,

        -- 🔹 المقاييس المشتقة
        DATEDIFF('year', se.birth_date, CURRENT_DATE()) AS age,
        DATEDIFF('year', se.hire_date, CURRENT_DATE()) AS years_of_service,

        -- 🔹 العنوان
        se.address,
        se.city,
        se.region,
       
        se.country,

        -- 🔹 بيانات المناطق (من التجميع)
        COALESCE(gt.total_territories_covered, 0) AS total_territories_covered,
        gt.territories_list,

        -- 🔹 البيانات المالية
        se.salary,
        se.home_phone,
       

    FROM stg_employees se
    LEFT JOIN grouped_territories gt ON se.employee_id = gt.employee_id
    WHERE se.employee_id IS NOT NULL
)


SELECT * FROM final
