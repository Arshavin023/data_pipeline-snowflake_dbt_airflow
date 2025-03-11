SELECT 
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    line_item.extended_price,
    {{ discounted_amount('line_item.extended_price','line_item.discount_percentage') }} as item_discount_amount
FROM {{ ref('stg_tpch_orders') }} orders 
JOIN {{ ref('stg_tpch_lineitem') }} line_item 
ON orders.order_key=line_item.order_key
ORDER BY orders.order_date