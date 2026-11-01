# The shop: one small dbt project that exercises the whole platform

Little data, real compute. Git holds two small seed files (200 customers, 30 products, about 12 KB). The order lines are
**generated inside Trino** from the line number (default 200 000 lines, seconds on the dev node), so the example needs no download
and stores nothing large. Then it is an ordinary dbt data model, one layer per catalog:

| Layer | Catalog | What | Materialization |
|---|---|---|---|
| bronze | `bronze.shop` | the seeds `customers`, `products` and the generated `order_lines` | table |
| silver | `silver.shop` | `stg_customers`, `stg_products`, `stg_order_lines` (clean names, types) | view |
| gold | `gold.shop` | `daily_revenue` (incremental merge), `customer_ltv` (window functions: rank, cumulative share), `product_mix` (share of revenue, rank in category) | table / incremental |

20 tests: unique, not null, relationships, accepted values, and two singular tests (revenue is never negative; the units of
the marts equal the units of the lines).

## Check it without a cluster

```bash
python3 -m venv .venv && .venv/bin/pip install dbt-core dbt-trino
make check DBT=.venv/bin/dbt      # dbt parse: 7 models, 2 seeds, 20 tests
```

## Run it on the platform

In a JupyterHub terminal (the notebook image has the platform's Trino address and your token):

```bash
pip install dbt-core dbt-trino
export DBT_TRINO_HOST=trino.stardelt.svc.cluster.local DBT_TRINO_JWT="$TRINO_TOKEN"
make demo
```

A token lives five minutes: set it and run right away; the whole run must finish inside that time (at the default scale it does,
see below). `make demo SCALE=1000000` builds a million lines. Then look at the results:

```sql
select * from gold.shop.daily_revenue order by order_date desc limit 7;
select * from gold.shop.customer_ltv order by value_rank limit 10;
select * from gold.shop.product_mix order by revenue desc limit 10;
```

Run `make demo` a second time: `daily_revenue` is merged, not rebuilt. `make teardown` drops everything the example created
(`bronze.shop`, `silver.shop`, `gold.shop`).

## Notes

- The catalogs `bronze`, `silver` and `gold` and the right to write to them come from the platform; a person outside the group that
  may write sees `Access Denied: Cannot access catalog gold`.
- `sequence` in Trino is limited to 10 000 entries, so the generator builds `scale` lines as 1000 blocks of 1000; `scale` is a
  multiple of 1000 and the model says so when it is not.
- The same project becomes a scheduled pipeline (a `dbt` task) and a bundle with `dev`, `test` and `prod` targets in the later
  slices of stardelt-demos#31.
- **Status:** the SQL parses as Trino SQL and the project parses in dbt, but nobody has run it against the live cluster yet. The first
  live run is the check; a failure is a bug in this example, file it.
