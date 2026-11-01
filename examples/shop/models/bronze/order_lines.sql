{#
  The compute of the example: `scale` order lines built inside Trino, nothing downloaded and nothing stored in Git.
  `sequence` is limited to 10 000 entries, so the numbers come from two small sequences (blocks of 1000 times 1000).
  Customer, product, quantity and day come from a hash of the line number: the same lines every run.
#}
{% set scale = var('scale') %}
{% if scale < 1000 or scale % 1000 != 0 %}
  {{ exceptions.raise_compiler_error("var scale must be a multiple of 1000, at least 1000; got " ~ scale) }}
{% endif %}

with blocks as (
    select x from unnest(sequence(0, {{ scale // 1000 - 1 }})) as t(x)
),
within_block as (
    select y from unnest(sequence(1, 1000)) as t(y)
),
numbered as (
    select x * 1000 + y as order_line_id from blocks cross join within_block
)
select
    order_line_id,
    1 + mod(mod(from_big_endian_64(xxhash64(to_utf8('c' || cast(order_line_id as varchar)))), 200) + 200, 200) as customer_id,
    1 + mod(mod(from_big_endian_64(xxhash64(to_utf8('p' || cast(order_line_id as varchar)))), 30) + 30, 30) as product_id,
    1 + mod(mod(from_big_endian_64(xxhash64(to_utf8('q' || cast(order_line_id as varchar)))), 5) + 5, 5) as quantity,
    date_add('day', mod(mod(from_big_endian_64(xxhash64(to_utf8('d' || cast(order_line_id as varchar)))), 365) + 365, 365), date '2025-01-01') as order_date
from numbered
