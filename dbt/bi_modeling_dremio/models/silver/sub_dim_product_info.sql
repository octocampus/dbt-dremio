
{{ config(
    materialized='incremental',
    unique_key='id',
    schema='silver'
) }}

with prods as (
    SELECT
    product_category_id as id,
    product_description as description,
    product_supplier as supplier,
    product_department as department,
    CURRENT_TIMESTAMP AS last_modified
FROM {{ source('layer_bronze', 'raw_sales') }}
)

select distinct * from prods

{% if is_incremental() %}
WHERE
    last_modified > (SELECT MAX(last_modified) FROM {{ this }} )
{% endif %}