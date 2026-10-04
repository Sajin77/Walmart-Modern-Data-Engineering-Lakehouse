-- This is an ephemeral model that selects distinct customers from the silver_b.obt_b model.
-- This is not a table or view (materialisation), but rather a temporary result set that can be used in other models or queries.
-- {{ config(materialized='ephemeral') }} is not required here, as its defined within the dbt_project.yml file, but it can be added for clarity.
SELECT 
    DISTINCT
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_phone,
    customer_city,
    customer_province,
    customer_country,
    customer_created_timestamp,
    customer_updated_timestamp,
    customer_is_active,
    customer_processed_at,
    CURRENT_TIMESTAMP() AS customer_gold_processed_at
FROM 
    {{ ref('obt_b') }}