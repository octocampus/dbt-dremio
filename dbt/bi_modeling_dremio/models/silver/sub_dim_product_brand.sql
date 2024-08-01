
{{ config(
    materialized='incremental',
    unique_key='id',
    schema='bi-modeling-warehouse.silver'
) }}

with prod_brands as (
    SELECT
    brand_id as id,
    brand_name as name,
    brand_description as description,
    brand_headquarters as headquarters,
    brand_employees_number as employees_number,
    CURRENT_TIMESTAMP AS last_modified
FROM {{ source('layer_bronze', 'raw_sales') }}
)

select distinct * from prod_brands

{% if is_incremental() %}
WHERE
    last_modified > (SELECT MAX(last_modified) FROM {{ this }} )
{% endif %}