create or replace view silver.${params.schema}.stg_products as
select
    id as product_id,
    name as product_name,
    category,
    cast(unit_price as decimal(10, 2)) as unit_price
from bronze.${params.schema}.products
