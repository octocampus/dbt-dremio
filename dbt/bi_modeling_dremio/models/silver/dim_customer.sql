{{ config(
    materialized='incremental',
    unique_key='id',
    schema='silver'
) }}

WITH ranked_sales AS (
    SELECT
        customer_id as id,
        customer_first_name as first_name,
        customer_last_name as last_name,
        customer_phone as phone,
        customer_email as email,
        customer_card_number as card_number,
        customer_card_type as card_type,
        customer_card_expiration_date as card_expiration_date,
        customer_card_delivery_bank as card_delivery_bank,
        CURRENT_TIMESTAMP AS last_modified,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY 
                CASE 
                    WHEN customer_card_number IS NOT NULL THEN 1
                    ELSE 2
                END
        ) AS custom_rank
    FROM {{ source('layer_bronze', 'raw_sales') }}
)

SELECT
    id,
    first_name,
    last_name,
    phone,
    email,
    card_number,
    card_type,
    card_expiration_date,
    card_delivery_bank,
    last_modified
FROM ranked_sales
WHERE custom_rank = 1

{% if is_incremental() %}
AND (
    last_modified > (SELECT MAX(last_modified) FROM {{ this }} )
    OR id IN (
        SELECT id
        FROM {{ this }}
        WHERE card_number IS NULL
    )
)
{% endif %}
