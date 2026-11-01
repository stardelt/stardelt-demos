select
    id as customer_id,
    name as customer_name,
    upper(country) as country,
    segment,
    cast(signup_date as date) as signup_date
from {{ ref('customers') }}
