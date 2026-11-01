select
    p.product_id,
    p.product_name,
    p.category,
    sum(l.quantity) as units,
    sum(l.quantity * p.unit_price) as revenue,
    sum(l.quantity * p.unit_price) / sum(sum(l.quantity * p.unit_price)) over () as revenue_share,
    rank() over (partition by p.category order by sum(l.quantity * p.unit_price) desc) as rank_in_category
from {{ ref('stg_order_lines') }} l
join {{ ref('stg_products') }} p on p.product_id = l.product_id
group by p.product_id, p.product_name, p.category
