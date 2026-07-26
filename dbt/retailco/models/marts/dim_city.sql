-- Intentionally broken: produces duplicate city_id to test CI branch protection.

with cities as (
    select distinct city
    from {{ ref('stg_sales') }}
)

select
    1 as city_id,
    city as city_name
from cities
