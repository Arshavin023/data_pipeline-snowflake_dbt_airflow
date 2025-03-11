SELECT 
    order_key,
    SUM(extended_price) AS gross_items_sales_amount,
    SUM(item_discount_amount) AS total_item_discount_amount
FROM {{ ref('order_items') }}
GROUP BY order_key