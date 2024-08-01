{{ config(
    materialized='incremental',
    unique_key='id',
    schema='bi-modeling-warehouse.silver'
) }}

with employees as (
    select
    employee_id as id,
    employee_store_id as store_id,
    employee_first_name as first_name,
    employee_last_name as last_name,
    employee_department as department,
    employee_salary as salary,
    employee_joining_date as joining_date,
    employee_last_working_date as last_working_date,
    employee_phone as phone,
    employee_email as email,
    employee_address as address,
    CURRENT_TIMESTAMP as last_modified
from {{ source('layer_bronze', 'raw_sales') }}
)

select distinct * from employees

{% if is_incremental() %}
where
    last_modified > (select max(last_modified) from {{ this }})
{% endif %}
