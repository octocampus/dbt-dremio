{{ config(
    materialized='incremental',
    unique_key='id',
    schema='bi-modeling-warehouse.silver'
) }}


with prods as (

SELECT
    product_id as id,
    product_name as name,
    product_price as price,
    product_discount as discount,
    product_category_id as category_id,
    brand_id,
    CURRENT_TIMESTAMP AS last_modified
FROM {{ source('layer_bronze', 'raw_sales') }}

)

select distinct * from prods

{% if is_incremental() %}
WHERE
    last_modified > (SELECT MAX(last_modified) FROM {{ this }} )
{% endif %}
