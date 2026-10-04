-- This is an ephemeral model that selects distinct products from the silver_b.obt_b model.
SELECT 
    DISTINCT
    product_id,
    product_name,
    category,
    brand,
    price,
    product_created_timestamp,
    product_updated_timestamp,
    product_is_active,
    product_processed_at,
    CURRENT_TIMESTAMP() AS product_gold_processed_at
FROM 
    {{ ref('obt_b') }}