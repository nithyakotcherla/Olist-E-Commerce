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
| [1](#phase-1--get-the-data) | Get the data | 9 Kaggle CSVs in `raw_data/` |
| [2](#phase-2--stand-up-the-infrastructure) | Stand up the infrastructure | 6 containers healthy |
| [3](#phase-3--seed-the-source-database) | Seed the source database | ~100k orders in the OLTP Postgres |
| [4](#phase-4--extract-validate-land-in-the-lake) | Extract, validate, land | Parquet partitions in MinIO |
| [5](#phase-5--load-the-lake-into-the-warehouse) | Load into the warehouse | 9 `raw_*` tables in the DW |
| [6](#phase-6--model-the-star-schema) | Model the star schema | 9 views + 6 mart tables |
| [7](#phase-7--gate-the-build-with-tests) | Gate with tests | `PASS=10` |
| [8](#phase-8--serve-it-in-metabase) | Serve it | A dashboard on the star schema |
| [9](#phase-9--prove-the-incremental-logic) | Prove the incremental logic | A late-arriving update merged, not rebuilt |
| [10](#phase-10--extend-it) | Extend it | The gaps closed, in priority order |

---

## Phase 0 — Orientation

> **How this repository came to exist.** This document was written first, as a spec, and
> the code was authored from it. Phases 0–2 are built: `docker-compose.yml`, `dags/` with
> the DAG and the `orders` contract, `init_oltp_db.py`, and the ignore rules. **The dbt
> project is not built yet** — Phase 6 authors it. Two of the Phase 10 items were applied
> up front because without them the build fails or destroys itself: `pyarrow` is pinned
> and named volumes are declared. Both are marked below.

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

### Where the diagram and the code disagree

These are not bugs you need to fix to get a working pipeline. They are things you must
know so you are not waiting for something that will never happen.

**1. Airflow does not trigger dbt.** The README's flowchart shows
`B -. Triggers .-> G` (Airflow → dbt models). `dags/elt_pipeline_minio.py` contains only
extract and load tasks — 9 tables × 2 tasks = 18 tasks, no dbt operator anywhere. **dbt is
run by hand from your terminal in Phase 6.** Closing this gap is the first item in Phase 10.

**2. The extract/load is full-refresh, not incremental.** In `load_from_minio_to_dw()`:

```python
df.to_sql(name=f"raw_{table_name}", con=engine, schema='public',
          if_exists='replace', index=False)
```

Every DAG run drops and rebuilds each `raw_*` table in full. The incremental story in this
project is true *only inside dbt* (`fact_sales`), not in the EL layer. That is a legitimate
design — the lake keeps the partitioned history — but do not describe the pipeline as
end-to-end incremental, because an interviewer who opens this file will see otherwise.

**3. The data contract is weaker than it looks.** `validate_data_contract()` runs
`df.astype(str)` before validating. Since every value becomes a string, every
`{"type": "string"}` assertion in `dags/contracts/orders.json` passes trivially. The only
checks doing real work are the `enum` on `order_status` and the `required` key list. It
also validates all ~100k records in a single `validate()` call, which is slow.

**4. Only one table has a contract.** `dags/contracts/` contains `orders.json` and nothing
else. The other 8 tables hit the `FileNotFoundError` branch, log
`⚠️ No contract found`, and pass through unvalidated. This is intentional and documented
in the README, but it means "the pipeline enforces data contracts" describes 1 of 9 tables.

**5. `dbt deps` is a no-op.** The README's run instructions include it, but there is no
`packages.yml` in `dbt_ecommerce/`. Harmless; it exits immediately.

**6. Nothing is persisted.** ✅ **Fixed.** `docker-compose.yml` declares six named volumes
— `oltp_data`, `dw_data`, `airflow_meta_data`, `airflow_logs`, `minio_data`,
`metabase_data` — so `docker compose down` no longer destroys the warehouse, the Airflow
metadata, or a Metabase dashboard. `docker compose stop` is still the cheaper habit while
working, and `down -v` still wipes everything.

### Prerequisites

- Docker Desktop, running, with roughly 8 GB allocated. Check with
  `docker info --format '{{.MemTotal}}'` — the default 4 GB will run this stack but leaves
  little headroom once Airflow and Metabase are both up.
- [uv](https://docs.astral.sh/uv/) on the host — this repo is uv-managed (Python 3.12)
- A Kaggle account, and an API token at `~/.kaggle/kaggle.json`
- ~3 GB free disk

---

## Phase 1 — Get the data

The repository ships no data. `raw_data/` is referenced by `docker-compose.yml` and
`init_oltp_db.py` but does not exist until you create it.

First, an API token. Kaggle downloads are authenticated even for public datasets:
<https://www.kaggle.com/settings> → **Create New Token**, which downloads `kaggle.json`.

```bash
mkdir -p ~/.kaggle && mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

Then install the host dependencies and pull the dataset:

```bash
uv sync
uv run kaggle datasets download -d olistbr/brazilian-ecommerce -p raw_data --unzip
```

That gives you the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
with the filenames matching exactly what `init_oltp_db.py` maps:

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

`AIRFLOW__CORE__EXECUTOR=LocalExecutor` and `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` are set
in the same block, and both matter. `airflow standalone` otherwise falls back to the
SequentialExecutor on SQLite, and the 18 tasks in Phase 4 will run one at a time instead of
in parallel.

**Verify:** all six services `Up`; `http://localhost:8080` shows the login page
(`airflow` / `airflow`); `http://localhost:9001` shows the MinIO console
(`minioadmin` / `minioadmin`).

Three checks worth doing now rather than discovering the answer in Phase 4:

```bash
docker compose exec airflow airflow config get-value core executor    # LocalExecutor
docker compose exec airflow python -c "import pyarrow; print(pyarrow.__version__)"
docker compose exec airflow airflow tasks list oltp_s3_olap_pipeline | wc -l   # 18
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
installed *inside* the Airflow container and pins what that Python 3.10 image needs
(pandas 2.2.3, `numpy<2.0.0`). `pyproject.toml` holds the host set — pandas, SQLAlchemy,
psycopg2-binary, kaggle — resolved by uv against this repo's Python 3.12. Installing the
container pins into the host venv, as an earlier draft of this doc suggested, downgrades
pandas for no reason and can fail outright.

dbt is in its own dependency group for the same reason the original two-step install
existed — resolving it alongside everything else produces conflicts:

```bash
uv sync --group dbt              # when you reach Phase 6
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
cross-table dependencies, so they parallelize under the `LocalExecutor`.

What each extract task does, in order:

1. `PostgresHook(postgres_conn_id='OLTP_DB').get_pandas_df("SELECT * FROM <table>;")` —
   a full read into a DataFrame.
2. **`validate_data_contract(df, table_name)`** — the interceptor. Looks for
   `dags/contracts/<table>.json`. Found → validate and raise `ValueError` on violation.
   Not found → log a warning and continue.
3. `df.to_parquet(buffer)` — serialize in memory, no temp files.
4. Create `staging-lake` if absent, then `put_object` under a Hive-style key:
   `<table>/dt=<YYYY-MM-DD>/<table>.parquet`, where the date is Airflow's `{{ ds }}`.

That partition key is the reason a re-run overwrites its own partition instead of
duplicating data, and the reason you can backfill a specific day without touching the
source database.

**Verify the contract actually fired.** In the Airflow UI open the logs for
`extract_orders_to_minio` and look for:

```
✅ Data Contract validation passed for orders.
```

Then check any other table's log — say `extract_products_to_minio` — and you should see
the `⚠️ No contract found` line instead. Seeing both is how you confirm you understand
gap 4 from Phase 0.

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

> **`dbt_ecommerce/` does not exist yet.** This phase authors it as well as running it —
> `dbt_project.yml`, `models/staging/` (9 views + `sources.yml`) and `models/marts/`
> (3 dims, 3 facts, `schema.yml`). The model contents are fully specified below; running
> `dbt run` before writing them will report no models found. Install dbt first with
> `uv sync --group dbt`.

### Configure the dbt profile

Not in the repository — you must create it. Either `~/.dbt/profiles.yml` or
`dbt_ecommerce/profiles.yml`:

```yaml
dbt_ecommerce:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: dw_admin
      password: dw_password
      port: 5434
      dbname: analytics_warehouse
      schema: public
      threads: 4
```

Port **5434** — the DW's published port. Using 5433 points dbt at the OLTP database, where
no `raw_*` tables exist, and you get a confusing "source not found" failure.

### Build

```bash
cd dbt_ecommerce
dbt debug                 # confirms the connection before you waste a build
dbt run --full-refresh
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

**Marts — 6 models, materialized as tables.**

| Model | Grain | Notes |
|---|---|---|
| `dim_customers` | one row per `customer_id` | `SELECT DISTINCT` |
| `dim_sellers` | one row per `seller_id` | `SELECT DISTINCT` |
| `dim_products` | one row per `product_id` | joins the category translation, `COALESCE`s to `'unknown'` |
| `fact_sales` | one row per order **line item** | **incremental** |
| `fact_payments` | one row per payment sequence | table |
| `fact_reviews` | one row per review | table |

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

```bash
dbt test
```

Expect exactly `PASS=10 WARN=0 ERROR=0 SKIP=0 TOTAL=10`. All ten are declared in
`models/marts/schema.yml`:

| # | Model | Column | Test | What it protects against |
|---|---|---|---|---|
| 1, 2 | `dim_customers` | `customer_id` | `unique`, `not_null` | A duplicated dimension row silently doubling every joined metric |
| 3, 4 | `dim_sellers` | `seller_id` | `unique`, `not_null` | The same, on the seller side |
| 5 | `fact_sales` | `order_id` | `not_null` | Orphan line items with no parent order |
| 6 | `fact_sales` | `customer_id` | `not_null` | Revenue that cannot be attributed |
| 7 | `fact_sales` | `customer_id` | `relationships` → `dim_customers` | A join that would drop rows — the classic silent revenue leak |
| 8 | `fact_sales` | `seller_id` | `not_null` | Unattributable seller performance |
| 9 | `fact_sales` | `seller_id` | `relationships` → `dim_sellers` | The same, seller side |
| 10 | `fact_sales` | `price` | `not_null` | A NULL that makes `SUM(price)` quietly wrong |

The two `relationships` tests are the valuable ones: they assert referential integrity that
Postgres is not enforcing, because the warehouse tables are created by pandas `to_sql` with
no foreign keys.

**Note what is *not* tested:** `dim_products`, `fact_payments` and `fact_reviews` have zero
tests. That is the cheapest meaningful contribution you can make to this repo — Phase 10,
item 2.

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

**3. Re-run the pipeline** — trigger the DAG again so the change reaches `raw_orders`.

**4. Run dbt incrementally** (no `--full-refresh`, which is the whole point):

```bash
cd dbt_ecommerce && dbt run --select fact_sales
```

**5. Observe.** Re-run the count query. `max(updated_at)` jumps to today. The row count
changes by the number of line items on that one order, not by 112,650. Read the dbt log
and you will see it processed a handful of rows rather than the full table.

Put the before/after numbers in your README. That is a result, not a claim.

---

## Phase 10 — Extend it

In priority order — the first two are what a reviewer would notice first.

**1. Make Airflow actually run dbt.** Closes the largest diagram-versus-code gap. Append to
`elt_pipeline_minio.py`, after the task loop:

```python
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt_ecommerce && dbt run',
)
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt_ecommerce && dbt test',
)
dbt_run >> dbt_test
```

This needs three supporting changes: mount `./dbt_ecommerce` into the Airflow container in
`docker-compose.yml`, install `dbt-postgres` inside that container, and collect the load
tasks into a list so you can express `load_tasks >> dbt_run` instead of leaving dbt
dangling. Doing it with `airflow.providers.common.sql` or Cosmos is nicer still, but the
`BashOperator` version is honest and readable.

**2. Test the three untested marts.** `dim_products.product_id` gets `unique` + `not_null`;
`fact_reviews.review_score` gets an `accepted_values` test for 1–5;
`fact_payments.payment_value` gets `not_null`. Six more tests, twenty minutes, and it makes
the suite coherent rather than partial.

**3. Write the remaining eight data contracts.** `dags/contracts/` has one file. Model the
new ones on `orders.json`, and while you are there, fix the `df.astype(str)` weakness from
Phase 0 by validating a typed sample instead of the stringified whole — it makes the type
assertions mean something and stops the 100k-record single-call validation.

**4. Use `geolocation`.** It is extracted, loaded, and staged as `stg_geolocation`, but no
mart consumes it. A `dim_geography`, or lat/long joined onto `dim_customers`, unlocks the
map visualizations the dataset is actually good for.

> Two items that were on this list are already done: `pyarrow` is pinned in
> `requirements.txt`, and named volumes are declared in `docker-compose.yml`. Both were
> applied when the repo was built — see the Phase 0 note.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Port 8080 refuses connection for several minutes | Airflow is still doing first-boot pip install + db migrate | `docker compose logs -f airflow` and wait |
| The 18 tasks run one at a time | `AIRFLOW__CORE__EXECUTOR` missing, so `standalone` fell back to SequentialExecutor | `airflow config get-value core executor` should say `LocalExecutor`; check the env block |
| `401 Unauthorized` from the `kaggle` command | No token, or wrong permissions on it | `~/.kaggle/kaggle.json`, `chmod 600` |
| dbt: source `olist.raw_orders` not found | Profile points at port 5433 (OLTP) instead of 5434 (DW) | Fix `profiles.yml`; confirm with `dbt debug` |
| Metabase cannot reach the database at `localhost:5434` | Metabase resolves names on the Docker network | Use host `postgres-dw`, port `5432` |
| `WARNING: File ... not found. Skipping <table>` | A CSV is renamed or missing | Match the exact Kaggle filenames from Phase 1 |
| `dbt run` rebuilds all of `fact_sales` every time | `--full-refresh` left in the command | Drop the flag after the first build |
| Containers exit or the stack is sluggish | Docker Desktop's default 4 GB is tight for six containers | Raise it to ~8 GB in Docker Desktop → Settings → Resources |

---

## Verification checklist

- [ ] 9 CSVs in `raw_data/`
- [ ] 6 containers `Up`
- [ ] `airflow config get-value core executor` returns `LocalExecutor`
- [ ] `airflow tasks list oltp_s3_olap_pipeline` returns 18 tasks
- [ ] `import pyarrow` succeeds inside the Airflow container
- [ ] `SELECT count(*) FROM orders` in the OLTP DB returns ~99,441
- [ ] DAG `oltp_s3_olap_pipeline` succeeds — all 18 tasks green
- [ ] `extract_orders_to_minio` log shows the contract **passing**
- [ ] Another table's log shows the contract being **bypassed**
- [ ] `staging-lake` holds 9 partitioned Parquet objects
- [ ] 9 `raw_*` tables in the warehouse
- [ ] `dbt run --full-refresh` builds 15 models
- [ ] `dbt test` reports `PASS=10`
- [ ] One test made to fail, then fixed, in its own commit
- [ ] Metabase dashboard reads marts, not `raw_*`
- [ ] Phase 9 done, with before/after row counts written down
