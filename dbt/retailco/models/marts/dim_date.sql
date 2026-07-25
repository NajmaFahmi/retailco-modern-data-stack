-- Date dimension: one row per unique order date with calendar attributes.


-- # Define Source Data
with dates as (
    select distinct order_date
    from {{ ref('stg_sales') }}
)


-- # Create Dimension Table (dim_date)
select 
    row_number() over (order by order_date) as date_id,
    order_date,
    extract(year from order_date) as year,
    extract(month from order_date) as month,
    extract(day from order_date) as day,
    format_date('%A', order_date) as day_of_week
from dates 