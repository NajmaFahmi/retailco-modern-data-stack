-- Sales fact table: one row per transaction.
-- Contains measures (amount, quantity, total_value) and foreign keys to dimensions.


-- # Define Source Data & Dimension Data 
with sales as (
    select * from {{ ref('stg_sales') }}
),

city_dim as (
    select city_id, city_name from {{ ref('dim_city') }}
),

product_dim as (
    select product_id, product_name from {{ ref('dim_product') }}
),

date_dim as (
    select date_id, order_date from {{ ref('dim_date') }}
)


-- # Create Fact Table (fact_sales)
select 
    sales.order_id,
    sales.customer_id,
    city_dim.city_id,
    product_dim.product_id,
    date_dim.date_id,
    sales.amount,
    sales.quantity,
    sales.total_value,
    sales.order_size
from sales
left join city_dim on sales.city = city_dim.city_name
left join product_dim on sales.product = product_dim.product_name
left join date_dim on sales.order_date = date_dim.order_date
