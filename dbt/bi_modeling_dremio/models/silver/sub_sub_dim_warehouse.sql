{{ config(
    materialized='incremental',
    unique_key='id',
    schema='bi-modeling-warehouse.silver'
) }}

with warehouses as (
    select
    employee_store_wh_id as id,
    employee_store_wh_name as name,
    employee_store_wh_address as address,
    employee_store_wh_city as city,
    employee_store_wh_postal_code as postal_code,
    employee_store_wh_country as country,
    CURRENT_TIMESTAMP as last_modified
from {{ source('layer_bronze', 'raw_sales') }}
)

select distinct * from warehouses

{% if is_incremental() %}
where
    last_modified > (select max(last_modified) from {{ this }})
{% endif %}