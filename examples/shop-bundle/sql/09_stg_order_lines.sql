create or replace view silver.${params.schema}.stg_order_lines as
select
    order_line_id,
    customer_id,
    product_id,
    quantity,
    order_date
from bronze.${params.schema}.order_lines
