-- City dimension: one row per unique city with a surrogate key.

-- # Define Source Data
with cities as (
    select distinct city
    from {{ ref('stg_sales') }}
)


-- # Create Dimension Table (dim_city)
select 
    row_number() over (order by city) as city_id,
    city as city_name
from cities