{{ 
    config(
        materialized='incremental',
        unique_key='OrderId',
        alias='orders'
    ) 
}}

with incremental_data as (
    SELECT * FROM {{ ref('raw_stg_orders') }}

    {% if is_incremental() %}

    WHERE ChangeTime > (SELECT COALESCE(MAX(ChangeTime),'1900-01-01') FROM {{ this }} )

    {% endif %}
),
final as (
    SELECT 
        DISTINCT OrderId,
        CustomerId,
        OrderDate,
        OrderStatus,
        ProductId,
        QuantityOrdered,
        Price,
        TotalAmount,
        ChangeTime,
        '{{ invocation_id }}' as batch_id
    FROM incremental_data
)

SELECT * FROM final
