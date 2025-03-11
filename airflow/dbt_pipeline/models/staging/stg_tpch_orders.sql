SELECT *
FROM {{ source('tpch', 'orders') }}