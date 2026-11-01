-- The units in the marts must equal the units in the lines they were built from.
select s.units as line_units, g.units as mart_units
from (select sum(quantity) as units from {{ ref('stg_order_lines') }}) s
cross join (select sum(units) as units from {{ ref('daily_revenue') }}) g
where s.units <> g.units
