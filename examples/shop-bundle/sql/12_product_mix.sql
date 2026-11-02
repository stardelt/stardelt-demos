create or replace table gold.${params.schema}.product_mix as
select
    p.product_id,
    p.product_name,
    p.category,
    sum(l.quantity) as units,
    sum(l.quantity * p.unit_price) as revenue,
    sum(l.quantity * p.unit_price) / sum(sum(l.quantity * p.unit_price)) over () as revenue_share,
    rank() over (partition by p.category order by sum(l.quantity * p.unit_price) desc) as rank_in_category
from silver.${params.schema}.stg_order_lines l
join silver.${params.schema}.stg_products p on p.product_id = l.product_id
group by p.product_id, p.product_name, p.category
