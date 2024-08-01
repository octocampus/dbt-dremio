{{
    config(
        materialized='incremental',
        unique_key='id',
        schema='bi-modeling-warehouse.silver'
    )
}}

select
    id,
    customer_id,
    product_id,
    employee_id,
    quantity,
    total_amount,
    total_discount,
    amount_paid,
    payment_mode,
    transaction_date,
    received_at,
    loaded_at
from
    {{ source('layer_bronze', 'raw_sales') }}


{% if is_incremental() %}
WHERE
    loaded_at > (SELECT MAX(loaded_at) FROM {{ this }} )
{% endif %}