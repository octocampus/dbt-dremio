{{ config(
    materialized='incremental',
    unique_key='id',
    schema='silver'
) }}

with stores as (
    select
    employee_store_id as id,
    employee_store_wh_id as warehouse_id,
    employee_store_name as name,
    employee_store_phone as phone,
    employee_store_email as email,
    employee_store_address as address,
    employee_store_city as city,
    CURRENT_TIMESTAMP as last_modified
from {{ source('layer_bronze', 'raw_sales') }}
)

select distinct * from stores

{% if is_incremental() %}
where
    last_modified > (select max(last_modified) from {{ this }})
{% endif %}