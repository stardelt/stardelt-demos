-- A singular test returns the rows that are wrong; it passes when it returns none.
select order_date, revenue from {{ ref('daily_revenue') }} where revenue < 0
