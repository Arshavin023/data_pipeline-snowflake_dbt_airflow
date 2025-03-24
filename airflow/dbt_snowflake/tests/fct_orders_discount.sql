SELECT * 
FROM {{ ref('fct_orders') }}
WHERE total_item_discount_amount > 0