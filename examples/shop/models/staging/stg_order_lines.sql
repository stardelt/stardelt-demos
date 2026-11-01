select
    order_line_id,
    customer_id,
    product_id,
    quantity,
    order_date
from {{ ref('order_lines') }}
