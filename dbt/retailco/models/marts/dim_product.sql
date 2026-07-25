-- Product dimension: one row per unique product with average price context.


-- # Define Source Data
with products as (
    select
        product,
        round(avg(amount), 2) as avg_price
    from {{ ref('stg_sales') }}
    group by product
)


-- # Create Dimension Table (dim_product)
select 
    row_number() over (order by product) as product_id,
    product as product_name,
    avg_price 
from products