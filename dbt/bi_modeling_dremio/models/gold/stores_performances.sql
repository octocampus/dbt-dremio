{{ config(
    materialized='incremental',
    unique_key='transaction_date',
    schema='gold'
) }}

WITH store_performance AS (
    SELECT
        DATE_TRUNC('day', fs.transaction_date) AS transaction_date,
        de.store_id AS store_id,
        SUM(fs.total_amount) AS total_revenue,
        COUNT(*) AS total_sales,
        SUM(fs.quantity) AS total_items_sold,
        CURRENT_TIMESTAMP AS last_modified
    FROM {{ ref('fact_sales') }} fs
    LEFT JOIN {{ ref('dim_employee') }} de ON fs.employee_id = de.id
    GROUP BY transaction_date, store_id
)

SELECT
    sp.transaction_date,
    sp.store_id,
    s.name AS store_name,
    s.city AS store_city,
    sp.total_revenue,
    sp.total_sales,
    sp.total_items_sold,
    sp.last_modified
FROM
    store_performance sp
LEFT JOIN
    {{ ref('sub_dim_store') }} s ON sp.store_id = s.id

{% if is_incremental() %}
WHERE sp.last_modified > (SELECT MAX(last_modified) FROM {{ this }})
{% endif %}
