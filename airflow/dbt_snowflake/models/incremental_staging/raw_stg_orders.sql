{{
    config(
        materialized='ephemeral'
    )
}}

with source_incremental_data as (
    SELECT 
        to_varchar(order_id) as OrderId,
        to_varchar(customer_id) as CustomerId,
        to_varchar(order_date) as OrderDate,
        to_varchar(status) as OrderStatus,
        to_varchar(product_id) as ProductId,
        to_varchar(Quantity) as QuantityOrdered,
        to_varchar(price) as Price,
        to_varchar(total_amount) as TotalAmount,
        -- to_timestamp(SUBSTR(cdc_timestamp,1,19),'YYYY-MM-DD HH:MM:SS') as ChangeTime
        to_timestamp_ntz(cdc_timestamp) as ChangeTime
    FROM {{ source('snowflake_incremental_source','raw_orders') }}
)
SELECT * FROM source_incremental_data