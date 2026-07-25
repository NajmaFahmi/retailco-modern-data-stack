-- Staging model for sales transactions.
-- Light transformations: select relevant columns and ensure consistent typing.



-- # Define Source Data
with source as (
    select * from {{ source('retailco', 'sales_clean') }}
)


-- # Select Data
-- no need to define the data type because bigquery read the schema from parquet
select 
    order_id,
    customer_id,
    product,
    city,
    amount,
    quantity,
    total_value,
    order_size,
    cast(order_date as date) as order_date      -- from str to date
from source 
