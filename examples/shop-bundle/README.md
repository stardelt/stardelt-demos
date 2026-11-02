# The shop as a bundle: publish it, run it, look at it, take it down

The same little shop as `examples/shop` (200 customers, 30 products, 200 000 generated order lines, one layer per catalog), but
as a **bundle of twelve SQL pipelines** instead of a dbt project. It needs no dbt, no extra image and no terminal on the cluster:
only the `stardelt` command line, signed in.

| Layer | Catalog | Pipelines | What |
|---|---|---|---|
| schemas | `bronze`, `silver`, `gold` | `shop-01-bronze`, `shop-02-silver`, `shop-03-gold` | one schema in each catalog |
| bronze | `bronze` | `shop-04-customers`, `shop-05-products`, `shop-06-lines` | the two small seed tables and the generated order lines |
| silver | `silver` | `shop-07-stg-cust`, `shop-08-stg-prod`, `shop-09-stg-lines` | three views with clean names and types |
| gold | `gold` | `shop-10-revenue`, `shop-11-ltv`, `shop-12-mix` | `daily_revenue`, `customer_ltv` (window functions), `product_mix` |

A pipeline runs exactly one statement, so every step of the shop is one pipeline; they run in name order, and the number in the
name is that order. Nothing is downloaded and nothing large is stored in Git: the order lines are built inside the query engine
from the line number (`scale` of them, a multiple of 1000, default 200 000).

## Before you start (an administrator, once)

- The catalogs `bronze`, `silver` and `gold` exist, and the group named in `bundle.yaml` (`ownerGroup: demo`) has a pipeline
  principal (`pipeline-demo`) that may write to them. In the platform repository: `make demo-access GROUP=demo`. For another
  group, change `ownerGroup` and `runAs` in `bundle.yaml`.
- You are a member of that group and have signed in: `stardelt login`.

## The loop

```bash
cd examples/shop-bundle
stardelt bundle validate .            # offline: twelve pipelines for each of the two targets
stardelt bundle plan . -t dev         # twelve lines "would-create"
stardelt bundle deploy . -t dev       # twelve pipelines, named dev-<your name>-shop-NN-..., no schedule
stardelt bundle run . -t dev          # starts them one after the other, waits for each, stops at the first that fails
stardelt bundle summary . -t dev      # what runs on the stage, from which commit
```

Then look at the results in the **SQL editor** (the tables of the `dev` target live in the schema `shop_dev`):

```sql
select * from gold.shop_dev.daily_revenue order by order_date desc limit 7;
select * from gold.shop_dev.customer_ltv order by value_rank limit 10;
select * from gold.shop_dev.product_mix order by revenue desc limit 10;
```

Every statement of the run is in your **Query log** with the source "Pipeline task". The **Deployments** page shows the bundle, the
target, the commit and who deployed it.

Change something (for example the `scale` variable in `bundle.yaml`), then:

```bash
stardelt bundle plan . -t dev --diff  # shows what would change in each pipeline
stardelt bundle deploy . -t dev
stardelt bundle run . -t dev --only shop-06-lines
```

`--only` takes the name with or without the `dev-<your name>-` prefix. After `scale` changed, run the pipelines from `shop-06-lines`
on again (the tables downstream are rebuilt from the lines).

## The two targets

- `dev` (mode `development`): your own copy, pipelines named `dev-<your name>-shop-NN-...`, no schedule, tables in the schema
  `shop_dev`. Pipeline names of a development copy are limited to 40 characters, so a sign-in name of up to 18 characters fits.
- `prod` (mode `production`, `runAs: pipeline-demo`): the shared copy an administrator publishes once, pipelines named
  `shop-NN-...`, tables in the schema `shop`. It has no schedule either; run it with `stardelt bundle run . -t prod`.

## Take it down

`stardelt bundle destroy . -t dev --yes` removes the pipelines. The tables stay (a pipeline cannot be told to drop what it made);
drop them in the SQL editor, one statement at a time:

```sql
drop view silver.shop_dev.stg_order_lines;
drop view silver.shop_dev.stg_products;
drop view silver.shop_dev.stg_customers;
drop table gold.shop_dev.product_mix;
drop table gold.shop_dev.customer_ltv;
drop table gold.shop_dev.daily_revenue;
drop table bronze.shop_dev.order_lines;
drop table bronze.shop_dev.products;
drop table bronze.shop_dev.customers;
drop schema gold.shop_dev;
drop schema silver.shop_dev;
drop schema bronze.shop_dev;
```

## How it differs from the dbt version

- No tests (dbt's twenty tests have no counterpart in a plain pipeline), and `daily_revenue` is rebuilt on every run instead of
  merging the last three days.
- Seeds are `values` lists inside two statements (`sql/04_customers.sql`, `sql/05_products.sql`), generated from the CSV files of
  `examples/shop/seeds`.
- The twelve statements were run in order against a local Trino 480 (with in-memory catalogs standing in for `bronze`, `silver` and
  `gold`, so `create or replace table` was run as drop and create) on 2026-11-02, and the checks below held. They were NOT run
  on a platform instance yet (Iceberg tables behind the catalog, the group's permissions, the Airflow loader): the first run
  there is the check (demos#31, slice A), and a failure is a bug in this example, file it. Times and memory are recorded here
  after that run, never guessed.

## What you should see

The data is built from hashes of the line number, so the numbers are the same on every run at the default `scale` of 200 000
(replace `shop_dev` by `shop` for the `prod` target):

```sql
select count(*), sum(quantity), min(order_date), max(order_date) from silver.shop_dev.stg_order_lines;
-- 200000, 600224, 2025-01-01, 2025-12-31
select count(*), sum(units), sum(revenue) from gold.shop_dev.daily_revenue;
-- 365, 600224, 36536658.10   (the units equal those of the lines; no day has a negative revenue)
select count(*) from gold.shop_dev.customer_ltv;      -- 200
select count(*) from gold.shop_dev.product_mix;       -- 30
select customer_name, lifetime_value from gold.shop_dev.customer_ltv where value_rank = 1;
-- Customer 053, 202135.50
```

Any other number after a run with the default `scale` means a statement did not do what it should.
