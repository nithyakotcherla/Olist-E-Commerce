# Implementation Guide

A phase-by-phase build order for this repository. Each phase names the files it touches,
the command that runs it, and the check that proves it worked before you move on.

Read [Phase 0](#phase-0--orientation) before running anything. It records where the
README's architecture diagram and the actual code disagree, and those gaps decide how
several later phases behave.

---

## Contents

| Phase | Name | Outcome |
|---|---|---|
| [0](#phase-0--orientation) | Orientation | You know what exists and what doesn't |
| [1](#phase-1--get-the-data) | Get the data | 9 CSVs in `raw_data/` |
| [2](#phase-2--stand-up-the-infrastructure) | Stand up the infrastructure | 6 containers healthy |
| [3](#phase-3--seed-the-source-database) | Seed the source database | ~100k orders in the OLTP Postgres |
| [4](#phase-4--extract-validate-land-in-the-lake) | Extract, validate, land | Parquet partitions in MinIO |
| [5](#phase-5--load-the-lake-into-the-warehouse) | Load into the warehouse | 9 `raw_*` tables in the DW |
| [6](#phase-6--model-the-star-schema) | Model the star schema | 9 views + 7 mart tables |
| [7](#phase-7--gate-the-build-with-tests) | Gate with tests | `PASS=24` |
| [8](#phase-8--serve-it-in-metabase) | Serve it | A dashboard on the star schema |
| [9](#phase-9--prove-the-incremental-logic) | Prove the incremental logic | A late-arriving update merged, not rebuilt |
| [10](#phase-10--extend-it) | Extend it | The gaps closed, in priority order |

---

## Phase 0 — Orientation

### What this project actually is

An ELT pipeline that moves the Olist Brazilian e-commerce dataset out of a simulated
production database, through an object-storage lake, into an analytics warehouse, where
dbt models it into a star schema that Metabase reads.

The point of the architecture is that analysts never touch the OLTP database. Every hop
exists to protect something: the lake gives you replayability, the contract gives you
schema-drift protection, the star schema gives you fast joins, the tests give you a
build that fails loudly instead of silently.

### The six containers

| Service | Image | Host port | Role |
|---|---|---|---|
| `postgres-oltp` | postgres:13 | 5433 | Simulated production database |
| `postgres-dw` | postgres:13 | 5434 | Analytics warehouse |
| `minio` | minio/minio | 9000 API, 9001 console | S3-compatible data lake |
| `postgres-airflow` | postgres:13 | — | Airflow's own metadata DB |
| `airflow` | apache/airflow:2.7.1-python3.10 | 8080 | Orchestration |
| `metabase` | metabase/metabase | 3000 | BI |

### Where the diagram and the code once disagreed

The six items below were the gaps between the README's flowchart and the code as
originally written. Four are now closed. They are kept here because knowing *why* each
one mattered is the point, and because two are still live.

**1. Airflow does not trigger dbt.** ✅ **Closed.** The DAG now collects every
`load_<table>_to_dw` task into a list and expresses `load_tasks >> dbt_run >> dbt_test`,
so the star schema is built and tested in the same run that lands the data. dbt itself
lives in a separate virtualenv at `/opt/airflow/dbt_venv`, provisioned on first boot,
because resolving `dbt-core` against Airflow's own pins in the same environment breaks
both.

**2. The extract/load is full-refresh, not incremental.** Still true, deliberately. Each
DAG run replaces every `raw_*` table in full; the incremental story lives inside dbt
(`fact_sales`), not in the EL layer. The lake keeps the partitioned history, so this is a
legitimate design — but do not describe the pipeline as end-to-end incremental, because
anyone who opens the DAG will see otherwise.

The load does *not* use `if_exists='replace'` any more, though. A plain replace issues
`DROP TABLE`, which Postgres refuses once dbt has built staging views on top:

```
cannot drop table raw_orders because other objects depend on it
```

`load_from_minio_to_dw()` now compares the incoming columns against the existing table.
Same layout → `TRUNCATE` + append, which gives identical full-refresh semantics without
breaking dependent objects. Genuinely changed layout → `DROP ... CASCADE` and let the
downstream `dbt_run` rebuild the views.

**3. The data contract is weaker than it looks.** ✅ **Closed.** The original
`validate_data_contract()` ran `df.astype(str)` before validating, so every value became a
string and every `{"type": "string"}` assertion passed trivially. `_json_safe()` replaces
it: only timestamps are stringified, numerics stay numeric, and NaN/NaT become `None` so
the `["string", "null"]` unions match. The `integer`, `number` and `minimum` assertions now
do real work.

It also validated all ~100k records in a single `validate()` call, which was the slowest
step in the extract task. Validation now runs on a seeded 2,000-row random sample
(`CONTRACT_SAMPLE_SIZE`). That trades exhaustiveness for speed — a violation in an
unsampled row gets through — which is the right trade for schema drift, since drift
affects every row, not one.

**4. Only one table has a contract.** ✅ **Closed.** All nine tables now have a contract in
`dags/contracts/`. The `FileNotFoundError` branch still exists and still logs
`⚠️ No contract found`, so adding a tenth table fails open rather than crashing.

**5. `dbt deps` is a no-op.** Still true. There is no `packages.yml` in `dbt_ecommerce/`,
so the command exits immediately. Harmless — the run instructions no longer include it.

**6. Nothing is persisted.** ✅ **Closed.** Named volumes are declared for
`postgres-oltp`, `postgres-dw`, `postgres-airflow` and `metabase`, so
`docker compose down` no longer destroys the warehouse or a Metabase dashboard.
`docker compose stop` is still the cheaper habit while working, and `down -v` still wipes
everything.

### Prerequisites

- Docker Desktop, running, with roughly 8 GB allocated. Check with
  `docker info --format '{{.MemTotal}}'` — on a 4 GB default the Metabase JVM gets
  OOM-killed (exit 137) when the load tasks spike, which is why `docker-compose.yml`
  caps it with `JAVA_OPTS: -Xmx768m` and `mem_limit: 2g`.
- [uv](https://docs.astral.sh/uv/) on the host — this repo is uv-managed (Python 3.12)
- A Kaggle account, *or* nothing at all: `generate_synthetic_olist.py` produces a
  stand-in dataset with the same shape
- ~3 GB free disk

---

## Phase 1 — Get the data

The repository ships no data. `raw_data/` is referenced by `docker-compose.yml` and
`init_oltp_db.py` but does not exist until you create it.

Two ways to fill it. Either download the
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
and extract it, or generate a stand-in that needs no Kaggle account:

```bash
uv sync
uv run python generate_synthetic_olist.py
```

The generator writes the same nine filenames with the same columns and the real row
counts (99,441 orders, 112,650 line items), with referential integrity intact so the dbt
tests pass. Geolocation is cut to 200k rows from the real ~1M, since every pandas
round-trip in the DAG pays for that table's size. Switching to the real data later means
extracting the Kaggle zip over `raw_data/` and re-running `init_oltp_db.py` — no pipeline
code changes.

Either way the filenames must match exactly what `init_oltp_db.py` maps:

```
raw_data/
├── olist_customers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

The names are hardcoded in the `csv_to_table_mapping` dict. A renamed file does not error —
it prints `WARNING: File ... not found. Skipping` and carries on, leaving you with a
missing table that only surfaces as a dbt failure three phases later.

**Verify:**

```bash
ls raw_data/*.csv | wc -l     # expect 9
```

---

## Phase 2 — Stand up the infrastructure

```bash
docker compose up -d
docker compose ps
```

Airflow's `command` block installs `requirements.txt`, migrates its metadata DB, creates
an admin user, and starts `airflow standalone` — all on first boot. **Expect 3–5 minutes**
before port 8080 answers. Watch it rather than guessing:

```bash
docker compose logs -f airflow
```

The two database connections Airflow needs are injected as environment variables in
`docker-compose.yml` (`AIRFLOW_CONN_OLTP_DB`, `AIRFLOW_CONN_DW_DB`), so there is nothing to
configure in the Airflow UI. That is why the DAG can call
`PostgresHook(postgres_conn_id='OLTP_DB')` with no setup step.

**Verify:** all six services `Up`; `http://localhost:8080` shows the login page
(`airflow` / `airflow`); `http://localhost:9001` shows the MinIO console
(`minioadmin` / `minioadmin`).

First boot also provisions dbt into its own virtualenv at `/opt/airflow/dbt_venv`, which
is why Phase 6 needs no manual install. That step is guarded by an `if [ ! -x ... ]`, so
restarts skip it.

Three checks worth doing now rather than discovering the answer in Phase 4:

```bash
docker compose exec airflow airflow config get-value core executor       # LocalExecutor
docker compose exec airflow python -c "import pyarrow; print(pyarrow.__version__)"
docker compose exec airflow /opt/airflow/dbt_venv/bin/dbt --version
```

---

## Phase 3 — Seed the source database

This runs on your **host**, not in a container — `init_oltp_db.py` connects to
`localhost:5433`, the published port of `postgres-oltp`.

```bash
uv sync                          # already done in Phase 1
uv run python init_oltp_db.py
```

**Host and container dependencies are deliberately separate.** `requirements.txt` is
installed *inside* the Airflow container and pins what its Python 3.10 image needs.
`pyproject.toml` holds the host set, resolved by uv against Python 3.12. Installing the
container pins into the host environment downgrades pandas for no reason.

dbt sits in its own dependency group for the same reason it gets its own virtualenv in
the container — resolving it alongside everything else conflicts:

```bash
uv sync --group dbt              # only if you want to run dbt from the host
```

The script reads each CSV with pandas and writes it to Postgres with
`if_exists="replace"`, so it is safely re-runnable.

**Verify:**

```bash
docker compose exec postgres-oltp psql -U admin -d olist_production -c "\dt"
docker compose exec postgres-oltp psql -U admin -d olist_production \
  -c "SELECT count(*) FROM orders;"     # expect ~99441
```

Nine tables: `customers`, `geolocation`, `orders`, `order_items`, `order_payments`,
`order_reviews`, `products`, `sellers`, `product_category_name_translation`.

---

## Phase 4 — Extract, validate, land in the lake

Open `http://localhost:8080`, unpause **`oltp_s3_olap_pipeline`**, and trigger it.

The DAG builds its task graph in a loop over the `TABLES` list, giving each table an
independent `extract_<table>_to_minio >> load_<table>_to_dw` chain — 18 tasks with no
cross-table dependencies, so they parallelize under the `LocalExecutor`. `max_active_tasks`
caps that at 4: nine tables loading concurrently, each buffering a full DataFrame, is
enough to get containers OOM-killed on a laptop.

Every load then fans into `dbt_run >> dbt_test`, so one trigger takes you from the source
database to a tested star schema — 20 tasks in total.

What each extract task does, in order:

1. `PostgresHook(postgres_conn_id='OLTP_DB').get_pandas_df("SELECT * FROM <table>;")` —
   a full read into a DataFrame.
2. **`validate_data_contract(df, table_name)`** — the interceptor. Looks for
   `dags/contracts/<table>.json`. Found → validate a 2,000-row sample and raise
   `ValueError` on violation. Not found → log a warning and continue. All nine tables have
   a contract, so the warning branch only fires if you add a tenth table.
3. `df.to_parquet(buffer)` — serialize in memory, no temp files.
4. Create `staging-lake` if absent, then `put_object` under a Hive-style key:
   `<table>/dt=<YYYY-MM-DD>/<table>.parquet`, where the date is Airflow's `{{ ds }}`.

That partition key is the reason a re-run overwrites its own partition instead of
duplicating data, and the reason you can backfill a specific day without touching the
source database.

**Verify the contract actually fired.** In the Airflow UI open the logs for
`extract_orders_to_minio` and look for:

```
✅ Data Contract validation passed for orders (2,000 of 99,441 records sampled).
```

Every table logs its own version of that line. To watch the interceptor actually bite,
break something on purpose — set one order's `order_status` to a value outside the enum in
the OLTP database and re-run the task. It should fail with
`❌ DATA CONTRACT VIOLATION in orders`, and nothing should reach the lake. Note you may
need to raise `CONTRACT_SAMPLE_SIZE` for a single bad row to land in the sample; that
limitation is Phase 10, item 2.

**Verify the lake:** MinIO console → Object Browser → `staging-lake`. Expect 9 prefixes,
each holding `dt=<today>/<table>.parquet`.

---

## Phase 5 — Load the lake into the warehouse

Handled by the same DAG run — the `load_<table>_to_dw` tasks. Each pulls its Parquet
object back from MinIO, reads it into pandas, and writes it to the DW as `raw_<table>`.

Reading back *from the lake* rather than passing the DataFrame directly between tasks is
the design choice worth being able to defend: the lake becomes the replay point, so a
warehouse rebuild never touches production again.

**Verify:**

```bash
docker compose exec postgres-dw psql -U dw_admin -d analytics_warehouse -c "\dt"
```

Expect 9 tables, all `raw_`-prefixed: `raw_customers`, `raw_orders`, `raw_order_items`,
`raw_products`, `raw_sellers`, `raw_order_payments`, `raw_order_reviews`,
`raw_geolocation`, `raw_product_category_name_translation`. These names are what
`dbt_ecommerce/models/staging/sources.yml` declares as the `olist` source.

---

## Phase 6 — Model the star schema

### The dbt profile is committed

`dbt_ecommerce/profiles.yml` is in the repository, which it has to be: the `dbt_run` task
passes `--profiles-dir /opt/airflow/dbt_ecommerce`, so a fresh clone would otherwise fail
inside Airflow. It carries no secret the compose file does not already publish, and it
resolves its host from the environment:

```yaml
host: "{{ env_var('DBT_HOST', 'localhost') }}"
port: "{{ env_var('DBT_PORT', '5434') | int }}"
```

`docker-compose.yml` sets `DBT_HOST=postgres-dw` and `DBT_PORT=5432` on the Airflow
service, so the same file works over the Docker network. From your host neither is set, so
it falls back to `localhost:5434` — the DW's published port. Port 5433 would point dbt at
the OLTP database, where no `raw_*` tables exist, and produce a confusing "source not
found" failure.

### Build

**Normally you do not run this by hand.** `dbt_run` and `dbt_test` are the last two tasks
in the DAG, so triggering the pipeline in Phase 4 already built the star schema. This
section is for when you want to iterate on a model without a full DAG run:

```bash
uv sync --group dbt
cd dbt_ecommerce
uv run dbt debug --profiles-dir .        # confirms the connection first
uv run dbt run --profiles-dir . --full-refresh
```

`--full-refresh` matters on the first build: `fact_sales` is incremental, and it needs to
exist before `is_incremental()` can be true.

### What gets built

**Staging — 9 models, materialized as views** (`dbt_project.yml`: `staging: +materialized: view`).
Views because these are cheap renaming-and-casting layers; there is no reason to persist them.

The one that carries real logic is `stg_orders.sql`, which manufactures a change-tracking
column the source data does not have:

```sql
GREATEST(
    CAST(order_purchase_timestamp AS TIMESTAMP),
    COALESCE(CAST(order_approved_at AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP)),
    COALESCE(CAST(order_delivered_carrier_date AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP)),
    COALESCE(CAST(order_delivered_customer_date AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP))
) AS updated_at
```

The most recent event anywhere in the order's lifecycle becomes its `updated_at`. Each
`COALESCE` falls back to the purchase timestamp so a NULL later stage cannot null the whole
expression. This synthetic high-water mark is what makes Phase 9 possible, and it is the
single most interview-worthy line in the repository.

**Marts — 7 models, materialized as tables.**

| Model | Grain | Notes |
|---|---|---|
| `dim_customers` | one row per `customer_id` | `SELECT DISTINCT` |
| `dim_sellers` | one row per `seller_id` | `SELECT DISTINCT` |
| `dim_products` | one row per `product_id` | joins the category translation, `COALESCE`s to `'unknown'` |
| `dim_geography` | one row per zip code prefix | averages the many coordinates per prefix; `MODE()` for city/state |
| `fact_sales` | one row per order **line item** | **incremental** |
| `fact_payments` | one row per payment sequence | table |
| `fact_reviews` | one row per review | table |

`dim_geography` exists because raw geolocation carries many rows per zip prefix — one per
captured coordinate — so it cannot be joined as a dimension without collapsing it first.
Averaging the lat/long and taking the modal city and state gives one row per prefix, and
`coordinate_count` keeps the sample size visible so you can tell a well-surveyed prefix
from a single stray reading.

`fact_sales` is the only incremental model:

```sql
{{ config(materialized='incremental', unique_key=['order_id', 'order_item_id']) }}
...
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

The composite `unique_key` is correct because the grain is the line item, not the order —
a two-item order produces two rows sharing an `order_id`. On an incremental run dbt filters
`stg_orders` to rows modified since the table's own high-water mark, then merges on that
composite key: existing line items are updated in place, new ones inserted.

**Verify:**

```bash
docker compose exec postgres-dw psql -U dw_admin -d analytics_warehouse \
  -c "SELECT count(*) FROM fact_sales;"          # expect ~112650
docker compose exec postgres-dw psql -U dw_admin -d analytics_warehouse \
  -c "SELECT count(*) FROM dim_customers;"       # expect ~99441
```

`fact_sales` exceeding `dim_customers` is correct and expected — line items outnumber
orders, and orders outnumber nothing here since Olist has one order per customer row.

---

## Phase 7 — Gate the build with tests

The DAG's `dbt_test` task already ran these. To run them yourself:

```bash
cd dbt_ecommerce && uv run dbt test --profiles-dir .
```

Expect `PASS=24 WARN=0 ERROR=0 SKIP=0 TOTAL=24`, all declared in
`models/marts/schema.yml`:

| Model | Column | Tests | What it protects against |
|---|---|---|---|
| `dim_customers` | `customer_id` | `unique`, `not_null` | A duplicated dimension row silently doubling every joined metric |
| `dim_sellers` | `seller_id` | `unique`, `not_null` | The same, on the seller side |
| `dim_products` | `product_id` | `unique`, `not_null` | The same, on the product side |
| `dim_products` | `category_name` | `not_null` | An untranslated category slipping through the `COALESCE` |
| `dim_geography` | `zip_code` | `unique`, `not_null` | The collapse-to-one-row-per-prefix logic silently regressing |
| `fact_sales` | `order_id` | `not_null` | Orphan line items with no parent order |
| `fact_sales` | `customer_id` | `not_null`, `relationships` → `dim_customers` | A join that would drop rows — the classic silent revenue leak |
| `fact_sales` | `seller_id` | `not_null`, `relationships` → `dim_sellers` | Unattributable seller performance |
| `fact_sales` | `product_id` | `not_null`, `relationships` → `dim_products` | Revenue that cannot be tied to a product |
| `fact_sales` | `price` | `not_null` | A NULL that makes `SUM(price)` quietly wrong |
| `fact_payments` | `order_id`, `payment_value` | `not_null` | NULLs silently breaking revenue sums |
| `fact_payments` | `payment_type` | `accepted_values` | A new payment method appearing unnoticed |
| `fact_reviews` | `review_id`, `order_id` | `not_null` | Reviews that cannot be attributed |
| `fact_reviews` | `review_score` | `not_null`, `accepted_values` 1–5 | A score outside the scale skewing every average |

The three `relationships` tests are the valuable ones: they assert referential integrity
that Postgres is not enforcing, because the warehouse tables are created by pandas
`to_sql` with no foreign keys.

**Make a test fail on purpose.** You do not understand a test suite you have never seen go
red. Add to `schema.yml` under `fact_sales`:

```yaml
      - name: freight_value
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
```

(or simpler, with no package dependency, add a `not_null` test to a column you know has
NULLs). Run `dbt test`, watch it fail, read the compiled SQL dbt prints, then fix it. Do
this in its own commit — a visible red-then-green in the history says more about you than
a suite that was always green.

---

## Phase 8 — Serve it in Metabase

`http://localhost:3000` → create an admin account → add a PostgreSQL database:

| Field | Value |
|---|---|
| Host | `postgres-dw` |
| Port | `5432` |
| Database | `analytics_warehouse` |
| Username | `dw_admin` |
| Password | `dw_password` |

**Host is `postgres-dw`, port `5432`** — not `localhost:5434`. Metabase connects over the
Docker network, where it sees the container name and the container-internal port. The
`localhost:5434` mapping only exists for processes on your host, like dbt in Phase 6. This
trips up nearly everyone once.

Point every question at the **marts**, never at `raw_*`. That is the entire justification
for Phases 4–7 — if your dashboard queries raw tables, you have built a star schema and
then declined to use it.

Worth building: revenue over time from `fact_sales`, category performance joining
`dim_products`, seller geography from `dim_sellers`, average review score from
`fact_reviews`, and payment-type mix from `fact_payments` — which conveniently exercises
all six marts.

> Metabase has no volume declared. Export or screenshot anything you build before running
> `docker compose down`, or it is gone.

---

## Phase 9 — Prove the incremental logic

Skipping this is the difference between "I used dbt incremental models" and "I can show you
one working." Nobody who clones this repo does it. It takes ten minutes.

**1. Record the current state:**

```bash
docker compose exec postgres-dw psql -U dw_admin -d analytics_warehouse \
  -c "SELECT count(*), max(updated_at) FROM fact_sales;"
```

**2. Simulate a late-arriving delivery update in the source.** An order that shipped
weeks ago finally gets its delivery confirmation — exactly the case the synthetic
`updated_at` was built for:

```bash
docker compose exec postgres-oltp psql -U admin -d olist_production -c "
UPDATE orders
SET order_delivered_customer_date = NOW()::text,
    order_status = 'delivered'
WHERE order_id = (SELECT order_id FROM orders WHERE order_status = 'shipped' LIMIT 1);"
```

**3. Re-run the pipeline** — trigger the DAG again. The change reaches `raw_orders`, and
the DAG's own `dbt_run` task rebuilds `fact_sales` incrementally, because it runs without
`--full-refresh` (which is the whole point).

To watch it in isolation instead:

```bash
cd dbt_ecommerce && uv run dbt run --profiles-dir . --select fact_sales
```

**5. Observe.** Re-run the count query. `max(updated_at)` jumps to today. The row count
changes by the number of line items on that one order, not by 112,650. Read the dbt log
and you will see it processed a handful of rows rather than the full table.

Put the before/after numbers in your README. That is a result, not a claim.

---

## Phase 10 — Extend it

The six items that used to live here are all done: Airflow runs dbt, all nine contracts
are written with typed sampled validation, every mart is tested, volumes are declared,
`pyarrow` is pinned, and `geolocation` feeds `dim_geography`. What follows is what is
genuinely still open, in priority order.

**1. Make the EL layer incremental.** This is the largest remaining gap and the one a
reviewer will find first. Every DAG run still extracts each table in full and rewrites the
whole `raw_*` table. `stg_orders` already manufactures an `updated_at` high-water mark —
push the same idea one layer up: track the last-loaded watermark per table, filter the
`SELECT` in `extract_to_minio()`, and append rather than truncate. The lake's `dt=`
partitioning already supports it; only the queries need to change.

**2. Validate the whole extract, cheaply.** `CONTRACT_SAMPLE_SIZE = 2000` trades
exhaustiveness for speed, so a violation in an unsampled row gets through. That is the
right trade for schema drift, which affects every row, but the wrong one for a bad value
in a single record. A vectorised check on the columns the contract constrains — dtype,
range, enum membership — would cover 100% of rows in less time than the current sample
takes.

**3. Run the tests in CI.** `dbt test` only gates a DAG run today, so a broken model can
be committed and pushed. A GitHub Actions workflow that spins up Postgres, seeds it from
`generate_synthetic_olist.py`, and runs `dbt build` would catch it at the PR.

**4. Join `dim_geography` onto the customer and seller dimensions.** The dimension exists
but nothing references it. A `zip_code` join onto `dim_customers` and `dim_sellers`
unlocks the map visualisations the dataset is actually good for.

**5. Add `dbt_utils` and source freshness.** There is still no `packages.yml`, so
`dbt deps` remains a no-op and tests like `accepted_range` and `expression_is_true` are
unavailable. A `freshness:` block in `sources.yml` would also catch a DAG that silently
stopped landing data.

**6. Stop shipping credentials in plaintext.** Every password lives in
`docker-compose.yml`. Fine for a local demo, and worth replacing with a `.env` file and
`env_file:` references before anyone mistakes the pattern for a recommendation.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Port 8080 refuses connection for several minutes | Airflow is still doing first-boot pip install + db migrate | `docker compose logs -f airflow` and wait |
| dbt: source `olist.raw_orders` not found | Profile points at port 5433 (OLTP) instead of 5434 (DW) | Fix `profiles.yml`; confirm with `dbt debug` |
| Metabase cannot reach the database at `localhost:5434` | Metabase resolves names on the Docker network | Use host `postgres-dw`, port `5432` |
| `WARNING: File ... not found. Skipping <table>` | A CSV is renamed or missing | Match the exact Kaggle filenames from Phase 1 |
| `dbt run` rebuilds all of `fact_sales` every time | `--full-refresh` left in the command | Drop the flag after the first build |
| `cannot drop table raw_orders because other objects depend on it` | An older load used `if_exists='replace'`, which issues `DROP TABLE` under dbt's views | Already handled — the load truncates and appends. If you see it, you are on a stale DAG file |
| Metabase container exits with code 137 | The JVM was OOM-killed | Already capped via `JAVA_OPTS` and `mem_limit`; raise Docker Desktop's memory if it persists |
| `dbt_run` fails with "Could not find profile" | `profiles.yml` missing from `dbt_ecommerce/` | It is committed — confirm the volume mount in `docker-compose.yml` |

---

## Verification checklist

- [ ] 9 CSVs in `raw_data/` (Kaggle or generated)
- [ ] 6 containers `Up`
- [ ] `SELECT count(*) FROM orders` in the OLTP DB returns ~99,441
- [ ] DAG `oltp_s3_olap_pipeline` succeeds — all 20 tasks green (18 extract/load + `dbt_run` + `dbt_test`)
- [ ] `extract_orders_to_minio` log shows the contract **passing**
- [ ] Every table's log shows its contract passing on a sampled batch
- [ ] `staging-lake` holds 9 partitioned Parquet objects
- [ ] 9 `raw_*` tables in the warehouse
- [ ] `dbt run --full-refresh` builds 16 models
- [ ] `dbt test` reports `PASS=24`
- [ ] One test made to fail, then fixed, in its own commit
- [ ] Metabase dashboard reads marts, not `raw_*`
- [ ] Phase 9 done, with before/after row counts written down
