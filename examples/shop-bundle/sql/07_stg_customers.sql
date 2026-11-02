create or replace view silver.${params.schema}.stg_customers as
select
    id as customer_id,
    name as customer_name,
    upper(country) as country,
    segment,
    cast(signup_date as date) as signup_date
from bronze.${params.schema}.customers
