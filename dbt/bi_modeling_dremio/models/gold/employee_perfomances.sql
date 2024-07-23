{{ config(
    materialized='incremental',
    unique_key='transaction_date',
    schema='gold'
) }}

WITH employee_performance AS (
    SELECT
        DATE_TRUNC('day', transaction_date) AS transaction_date,
        employee_id,
        SUM(total_amount) AS total_revenue,
        COUNT(*) AS total_sales,
        SUM(quantity) AS total_items_sold,
        CURRENT_TIMESTAMP AS last_modified
    FROM {{ ref('fact_sales') }}
    GROUP BY DATE_TRUNC('day', transaction_date), employee_id
)

SELECT
    emp.transaction_date,
    emp.employee_id,
    CONCAT(dim_emp.first_name, ' ', dim_emp.last_name) AS employee_name,
    emp.total_revenue,
    emp.total_sales,
    emp.total_items_sold,
    emp.last_modified
FROM
    employee_performance emp
LEFT JOIN
    {{ ref('dim_employee') }} dim_emp ON emp.employee_id = dim_emp.id

{% if is_incremental() %}
WHERE emp.last_modified > (SELECT MAX(last_modified) FROM {{ this }})
{% endif %}
