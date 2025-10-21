{% macro clean_phone(field_name) %}

    NULLIF(REGEXP_REPLACE(TRIM({{ field_name }}), '[^0-9]', ''), '')
{% endmacro %}



{% macro clean_postal_code(field_name) %}
  
    
    NULLIF(UPPER(TRIM({{field_name}})), '')

{% endmacro %}



{% macro standardize_country(field_name) %}
    
    CASE 
        WHEN UPPER(TRIM({{ field_name }})) IN ('UK', 'GB') THEN 'UNITED KINGDOM'
        WHEN UPPER(TRIM({{ field_name }})) = 'USA' THEN 'UNITED STATES' 
        ELSE INITCAP(TRIM({{ field_name }}))
    END

{% endmacro %}


{% macro standardize_name(field_name) %}
 
    INITCAP( REGEXP_REPLACE(NULLIF(TRIM({{ field_name }}), '') , '\s+', ' '))

{% endmacro %}