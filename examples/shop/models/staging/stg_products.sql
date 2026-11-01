select
    id as product_id,
    name as product_name,
    category,
    cast(unit_price as decimal(10, 2)) as unit_price
from {{ ref('products') }}
