{# Incremental: a later run recomputes the last three days and merges them in. #}
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_date') }}

select
    l.order_date,
    count(*) as order_lines,
    sum(l.quantity) as units,
    sum(l.quantity * p.unit_price) as revenue
from {{ ref('stg_order_lines') }} l
join {{ ref('stg_products') }} p on p.product_id = l.product_id
{% if is_incremental() %}
where l.order_date >= (select date_add('day', -3, max(order_date)) from {{ this }})
{% endif %}
group by l.order_date
