SELECT 
    stg_orders.*,
    order_items_summary.gross_items_sales_amount,
    order_items_summary.total_item_discount_amount
FROM {{ ref('stg_tpch_orders') }} stg_orders
JOIN {{ ref('order_items_summary') }} order_items_summary
ON stg_orders.order_key=order_items_summary.order_key
ORDER BY stg_orders.order_date 