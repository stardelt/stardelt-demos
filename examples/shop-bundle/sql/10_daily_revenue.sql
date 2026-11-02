-- Rebuilt on every run (the dbt version of the shop merges the last three days instead).
create or replace table gold.${params.schema}.daily_revenue as
select
    l.order_date,
    count(*) as order_lines,
    sum(l.quantity) as units,
    sum(l.quantity * p.unit_price) as revenue
from silver.${params.schema}.stg_order_lines l
join silver.${params.schema}.stg_products p on p.product_id = l.product_id
group by l.order_date
