{{ config(
    materialized='incremental',
    unique_key='product_id,transaction_date',
    schema='gold'
) }}

WITH sales_data AS (
    SELECT
        fs.product_id,
        pd.name,
        pd.price,
        COUNT(*) AS total_sales,
        SUM(fs.quantity) AS total_quantity,
        SUM(fs.total_amount) AS total_revenue,
        DATE_TRUNC('day', fs.transaction_date) AS transaction_date,
        CURRENT_TIMESTAMP AS last_modified
    FROM {{ ref('fact_sales') }} as fs
    LEFT JOIN {{ ref('dim_product') }} as pd ON fs.product_id = pd.id
    GROUP BY fs.product_id, pd.name, pd.price, transaction_date
)

SELECT * FROM sales_data

{% if is_incremental() %}
WHERE fs.last_modified > (SELECT MAX(last_modified) FROM {{ this }})
{% endif %}
