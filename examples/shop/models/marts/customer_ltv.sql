{# Window functions: each customer's lifetime value, their rank, and the share of revenue the best customers make up. #}
with per_customer as (
    select
        l.customer_id,
        count(*) as order_lines,
        sum(l.quantity * p.unit_price) as lifetime_value,
        min(l.order_date) as first_order,
        max(l.order_date) as last_order
    from {{ ref('stg_order_lines') }} l
    join {{ ref('stg_products') }} p on p.product_id = l.product_id
    group by l.customer_id
)
select
    c.customer_id,
    c.customer_name,
    c.country,
    c.segment,
    pc.order_lines,
    pc.lifetime_value,
    pc.first_order,
    pc.last_order,
    rank() over (order by pc.lifetime_value desc) as value_rank,
    sum(pc.lifetime_value) over (order by pc.lifetime_value desc rows between unbounded preceding and current row)
        / sum(pc.lifetime_value) over () as cumulative_share
from {{ ref('stg_customers') }} c
join per_customer pc on pc.customer_id = c.customer_id
