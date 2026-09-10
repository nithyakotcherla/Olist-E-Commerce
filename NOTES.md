# E-Commerce ELT Pipeline — Study Notes

Everything covered in the walkthrough: architecture, ELT concepts, dbt modelling,
incremental loading, and the gaps between this project and real production systems.

---

## 1. What This Project Is

An **ELT pipeline** that moves ~100,000 Brazilian e-commerce orders (Olist Kaggle dataset)
from a simulated production database, through a data lake, into an analytics warehouse,
where dbt reshapes it into a star schema that Metabase reads.

### The business problem

A production database is optimised for *writes* — "insert this order." It is bad at
analytics — "revenue by category for 2 years" needs huge scans and joins.

Running analytical queries directly on production **slows down the website for real
customers**. So you copy the data elsewhere and reshape it for analysis.

### Why every hop exists

| Hop | Buys you |
|---|---|
| Production -> Lake | Replayability; production is never hit twice |
| Data contract | Schema-drift protection (fail loudly, not silently) |
| Lake -> Warehouse | A SQL engine that can actually answer questions |
| Star schema | Fast, simple joins for analysts |
| dbt tests | A build that fails instead of quietly going wrong |

---

## 2. ELT vs ETL

The **E and L order never changes**. Only the **T** moves.

```
ETL:  Extract -> Transform -> Load     (clean it, then store it)
ELT:  Extract -> Load -> Transform     (store it, then clean it)
```

- **Extract** = getting data OUT of the source
- **Load** = putting data INTO the destination
- **Transform** = cleaning and reshaping

Old tools had weak databases, so you cleaned data mid-flight before loading (ETL).
Modern warehouses are cheap and powerful, so you dump raw data in and clean it with SQL
in place (ELT) — which also means **you keep the raw copy forever** and can redo the
cleaning whenever your logic changes.

### Naming is by endpoint, not by hop

In this project:

- **Extract** = production DB -> S3 (`extract_<table>_to_minio`)
- **Load** = S3 -> warehouse (`load_<table>_to_dw`)

S3 is **not an endpoint** — it is a rest stop in the middle of the E->L trip.

> Analogy: moving house. Stuff goes from old house -> storage unit -> new house.
> That is still one move-out and one move-in. The storage unit did not turn the
> move-out into a move-in.

The task order in the DAG is `extract_task >> load_task` — extract always first.
You cannot load what you have not fetched.

---

## 3. The Infrastructure (6 Docker Containers)

| Container | Pretending to be | Port | Role |
|---|---|---|---|
| `postgres-oltp` | Production database | 5433 | Source of truth. Analysts never touch it. |
| `minio` | Amazon S3 | 9000 / 9001 | Data lake (object storage) |
| `postgres-dw` | Snowflake / Redshift | 5434 | Analytics warehouse |
| `airflow` | itself | 8080 | Orchestration |
| `postgres-airflow` | — | — | Airflow's own bookkeeping DB |
| `metabase` | Tableau / Looker | 3000 | BI dashboards |

- **OLTP** = Online **Transaction** Processing (the website's DB)
- **OLAP** = Online **Analytical** Processing (the warehouse)

Two different jobs, two different databases.

### Port gotcha

- dbt connects from your **host** -> `localhost:5434`
- Metabase connects over the **Docker network** -> host `postgres-dw`, port `5432`

Metabase cannot see `localhost:5434`. That mapping only exists for processes on your machine.

---

## 4. The 9 Tables

| # | Table | Approx rows | What it holds | Feeds |
|---|---|---|---|---|
| 1 | `orders` | ~99,400 | One row per order: status + 4 lifecycle timestamps | `fact_sales` |
| 2 | `order_items` | ~112,650 | One row per **line item** — price, freight, seller | `fact_sales` |
| 3 | `customers` | ~99,400 | Customer id, city, state, zip | `dim_customers` |
| 4 | `products` | ~32,950 | Product id, category, dimensions | `dim_products` |
| 5 | `sellers` | ~3,100 | Seller id, city, state | `dim_sellers` |
| 6 | `order_payments` | ~103,900 | Payment type, installments, value | `fact_payments` |
| 7 | `order_reviews` | ~99,200 | 1-5 score, comment, timestamps | `fact_reviews` |
| 8 | `product_category_name_translation` | 71 | Portuguese -> English categories | `dim_products` (join) |
| 9 | `geolocation` | ~1,000,000 | Zip prefix -> lat/long | **nothing (unused)** |

### Three things to notice

**`orders` vs `order_items` is the crucial pair.** `orders` = the transaction (~99k).
`order_items` = the things inside it (~112k). The gap is multi-item orders. This is why
`fact_sales` has ~112,650 rows and why its unique key needs **both** columns.

**The translation table exists because the data is Brazilian** — categories are stored in
Portuguese (`cama_mesa_banho`). `dim_products` joins it for English, `COALESCE`-ing to
`'unknown'` where no translation exists.

**`geolocation` is the biggest table and is completely unused.** ~1M rows extracted,
uploaded, loaded and staged every run — then no mart consumes it. It is why DAG runs take
longer than expected.

---

## 5. The Data Journey

```
postgres-oltp  --[EXTRACT]-->  MinIO/S3  --[LOAD]-->  postgres-dw
                                                          |
                                                    [TRANSFORM]  <- dbt
                                                          |
                                                          v
                                                    star schema -> Metabase
```

### Step 0 — Seed the source
`init_oltp_db.py` loads 9 Kaggle CSVs into `postgres-oltp`. This fakes "a production
database that already has data." Run once, by hand.

### Step 1 — Extract
`SELECT * FROM <table>` into a pandas DataFrame. Full table scan, every run.

### Step 2 — Validate the data contract
Checks data against a rulebook **before** anything is saved. It is a *bouncer at the door*,
not a cleaner — it rejects or lets through, it **never modifies a value**.

### Step 3 — Land in the lake
Saved as **Parquet** (compressed, column-oriented — smaller and faster than CSV) into MinIO:

```
staging-lake/orders/dt=2026-09-07/orders.parquet
```

The `dt=` folder naming is **Hive-style partitioning**. Two payoffs:
- Re-running today overwrites *today's* folder instead of duplicating (**idempotent**)
- You keep dated history you can replay from without touching production

### Step 4 — Load into the warehouse
Reads the Parquet back **out of MinIO** and writes it to `postgres-dw` as `raw_orders`, etc.

> Reading it back from the lake instead of passing the DataFrame in memory looks wasteful
> but is deliberate: **the lake becomes the replay point.**

9 tables x 2 tasks = **18 Airflow tasks**, built in a `for` loop.

### IMPORTANT: nothing is cleaned before S3

The data landing in S3 is **raw**. Same messy columns, text-typed timestamps, NULLs.
Cleaning happens later, in dbt, in the warehouse. That is the "L before T" in ELT:
land it raw, transform after. If you clean on the way in and get it wrong, you have
destroyed the original.

---

## 6. Data Contracts

### It is NOT Great Expectations

This project uses **`jsonschema`** — a generic JSON validator — with hand-written
JSON Schema files in `dags/contracts/`.

| | `jsonschema` (this project) | Great Expectations |
|---|---|---|
| What it is | Generic JSON validator | Purpose-built data quality framework |
| Checks | Structure: does the column exist? is the value in this enum? | Statistical: is the mean in range? did row count drop 40%? |
| Output | Pass, or an exception | HTML reports, run history, profiling |
| Weight | One import + one JSON file | A whole framework |

A **schema contract** ("the shape is what I agreed to") is not a **data quality suite**
("the values look statistically sane"). Both are legitimate; different questions.

### Contract vs tool

A **data contract** is a *practice* — a formal agreement between the team producing data
and the team consuming it. Great Expectations is a *tool*. You can implement a contract
with `jsonschema`, GE, or Pydantic.

### What the contract actually checks

`orders.json` asserts column presence, types, and an `enum` on `order_status`.
If an upstream engineer drops a column, the pipeline **halts** instead of silently
corrupting dashboards. This is called **shift-left data quality** — catch problems at the
door, not three layers downstream.

---

## 7. Airflow

Airflow schedules **DAGs** — Directed Acyclic Graphs. Plain English: *"a to-do list where
some items must happen before others, and nothing loops back on itself."*

The dependency operator is `>>`, read as **"must finish before"**:

```python
extract_task >> load_task
```

Airflow gives you: a web UI to trigger runs, green/red task boxes, per-task logs, and
retry-only-the-failed-step.

`{{ ds }}` is Airflow's template for the run's date (YYYY-MM-DD) — it is what builds the
`dt=` partition path.

---

## 8. dbt

### dbt does NOT compute anything

This is the single most misunderstood thing about dbt. **dbt is a code generator.**
It takes your SQL files, works out the dependency order, and **sends SQL to a database to
execute**. All actual work happens in the database.

> dbt is the conductor. It does not play an instrument.

This is why you cannot "run dbt on S3" — S3 has no SQL. It is a bucket of files. You can
`PUT` and `GET`; you cannot `SELECT ... GROUP BY`.

### But you CAN skip the warehouse — with a query engine

Bolt a SQL engine onto S3 and you genuinely do not need a separate warehouse. This is the
**lakehouse** pattern:

| Engine | What it does |
|---|---|
| **AWS Athena** | Serverless SQL directly over S3. Pay per query. |
| **Trino / Presto** | Distributed SQL over object storage |
| **Databricks** | Spark-based, biggest name in the space |
| **DuckDB** | Single-machine, queries Parquet on S3 directly |

dbt has adapters for all of them (`dbt-athena`, `dbt-trino`, `dbt-databricks`, `dbt-duckdb`).

**Why this project uses a warehouse anyway:**
1. The engine still has to exist — you swapped a component, not removed one
2. Warehouses are faster: indexes, sorted storage, column stats, cached plans.
   Querying loose files means re-reading and re-parsing every time.
3. It teaches the classic pattern most companies actually run

**Bottom line: you can skip the warehouse, but only by adding a query engine.
S3 alone cannot answer a question.**

### The three layers

| Layer | Count | Does dbt create it? | Materialized as |
|---|---|---|---|
| Sources (`raw_*`) | 9 | **No** — Airflow made these | (already exist) |
| Staging (`stg_*`) | 9 | Yes | **views** |
| Marts (`dim_*`, `fact_*`) | 6 | Yes | **tables** |

`dbt run` builds **15 models**, not 24 — because sources are not models.

### Sources are declarations, not builds

A source is a **pointer** saying "this table already exists, someone else made it."
It is a label on someone else's work.

The dbt source is **NOT S3** — dbt has no idea S3 exists. `sources.yml` declares
`schema: public` and the 9 `raw_*` tables **in the warehouse**.

```
Airflow's job ends here --+
                          v
production -> S3 -> [ raw_* tables in postgres-dw ] -> dbt's job starts here
```

`raw_*` is the **handoff point**. Airflow writes it; dbt reads it. Neither knows about the
other's world. That clean seam means you could swap MinIO for real S3, or Airflow for
Dagster, and dbt would not change one line.

Payoff of declaring sources: `{{ source('olist', 'raw_orders') }}` instead of a hardcoded
name — gives you lineage in the docs, and a rename becomes one YAML edit instead of nine.

### Why views for staging, tables for marts

**A view stores no data.** It is a saved `SELECT`. "Rebuilding" a view just rewrites the
query definition — instant, regardless of row count. The data is computed fresh whenever
something queries it.

- **Staging = views** — thin renaming/casting layers; no reason to spend storage
- **Marts = tables** — real joins, hit repeatedly by dashboards; do the work once

---

## 9. The Star Schema

The key data-modelling idea:

- **Fact tables** = the events, the things you **measure**.
  `fact_sales`, `fact_payments`, `fact_reviews`.
  Long and skinny, millions of rows, full of numbers.
- **Dimension tables** = the descriptions, the things you **slice by**.
  `dim_customers`, `dim_products`, `dim_sellers`.
  Short and wide, full of labels.

Drawn out it looks like a star: facts in the middle, dimensions around the edges.

"Revenue by product category last month" becomes **one fact joined to one dimension** —
fast and obvious — instead of a seven-table join across normalised production tables.

### The 6 marts

| Model | Grain | Materialization |
|---|---|---|
| `dim_customers` | one row per `customer_id` | table |
| `dim_sellers` | one row per `seller_id` | table |
| `dim_products` | one row per `product_id` | table |
| `fact_sales` | one row per order **line item** | **incremental** |
| `fact_payments` | one row per payment sequence | table |
| `fact_reviews` | one row per review | table |

---

## 10. Incremental Loading — The Core Idea

### The problem

Rebuilding 112,650 rows every night is wasteful. You want to process only what changed.
But "what changed?" needs a `last_updated` column — **and the source data does not have one.**

### The synthetic `updated_at`

`stg_orders.sql` manufactures one:

```sql
GREATEST(
    CAST(order_purchase_timestamp AS TIMESTAMP),
    COALESCE(CAST(order_approved_at AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP)),
    COALESCE(CAST(order_delivered_carrier_date AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP)),
    COALESCE(CAST(order_delivered_customer_date AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP))
) AS updated_at
```

**Reasoning:** an order has four lifecycle timestamps — purchased, approved, handed to
carrier, delivered. They fill in over time. **The latest one populated IS the last time
anything happened to that order.** `GREATEST` picks it.

**Why the `COALESCE` wrappers matter:** in Postgres `GREATEST(x, NULL)` returns `NULL` —
one NULL poisons the whole expression. An in-flight order has NULLs in later columns, so
without `COALESCE` every undelivered order would get `updated_at = NULL` and never sync.
Falling back to the purchase timestamp makes the worst case "watermark = purchase time,"
which is correct.

### The watermark (high-water mark)

**dbt does not compare anything.** There is no diff between raw and `fact_sales` — that
would cost as much as rebuilding. It uses a **bookmark**.

> Like a bookmark in a book. You do not re-read from page 1 to find where you stopped —
> you look at where the bookmark sits and read forward.

```sql
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

| Piece | Meaning |
|---|---|
| `{% if is_incremental() %}` | Only if the table exists and we are not `--full-refresh`-ing |
| `{{ this }}` | `fact_sales` itself — dbt's self-reference |
| `SELECT MAX(updated_at) FROM {{ this }}` | **The bookmark.** One number: my newest timestamp |
| `WHERE updated_at > ...` | Filter source to rows newer than the bookmark |

Two questions, no comparison. The inner `SELECT MAX(...)` returns a single value.

### `updated_at` is a VALUE, not a POSITION

Common confusion: *"what if row 50 changes and the bookmark is at 100?"*

When row 50 is modified, its `updated_at` becomes **`NOW()`**. It does not stay at 50.
It jumps to 105 and lands **in front of** the bookmark.

```
Before the edit:            After the edit:
  row 50  -> updated_at 50    row 50  -> updated_at 105   <- jumped ahead
  bookmark = 100              bookmark = 100
                              105 > 100  -> caught
```

dbt never looks for "row 50." It only asks: *"which rows have `updated_at` > 100?"*
**Position in the original timeline is irrelevant. Only "when was this last touched."**

This is exactly why `GREATEST` watermarks on **lifecycle** columns, not purchase date.
A 2016 order delivered today jumps to the front of the queue:

```sql
GREATEST(purchase=2016-09-01, approved=2016-09-02, carrier=2016-09-03, delivered=2026-09-07)
                                                              -> updated_at = 2026-09-07
```

Watermarking on a **creation** date would be broken — a 2016 purchase date sits behind the
bookmark forever. This lifecycle-max trick is the most interview-worthy line in the repo.

### dbt READS the timestamp — it never writes it

**Rule to memorise: no upstream signal -> no detection.**

| Where | How `updated_at` is set |
|---|---|
| **Production DB** | A trigger, `ON UPDATE CURRENT_TIMESTAMP`, or the app's ORM |
| **This project** | `GREATEST()` derives it from lifecycle columns production wrote |
| **dbt** | **Never** |

---

## 11. Merge vs Append

`fact_sales` is configured with a composite key:

```sql
{{ config(materialized='incremental', unique_key=['order_id', 'order_item_id']) }}
```

### What dbt actually executes

1. Build a **temp table** from your SELECT, filtered by the watermark — say 40 rows
2. `MERGE` it into `fact_sales`:
   - key already exists -> **UPDATE in place**
   - key is new -> **INSERT**
3. Drop the temp table

### Why the key is composite

**The grain is the line item, not the order.** A 2-item order is 2 rows sharing one
`order_id`. Match on `order_id` alone and you would collapse them.

### The difference that matters

On a shipped -> delivered change:

| | Times processed | Rows in the table |
|---|---|---|
| **merge** | 2 | **1** (updated in place) |
| **append** | 2 | **2** (duplicate) |

The row passes through the transformation twice either way. What differs is what happens
on arrival. With `append` you would have the same line item twice and `SUM(price)` would
double-count.

### Key reframe

"Processed twice" sounds wasteful, but incremental loading is **not** trying to
*"never touch a row twice."* It is trying to *"not touch **all** rows every time."*
Reprocessing 2 rows out of 112,650 is a rounding error.

**This is the justification for the lookback window** — you deliberately reprocess a few
days of overlap because `MERGE` makes redundant work harmless, while missing a row is not.

> **Cheap redundancy beats silent data loss.** This instinct separates a pipeline that
> works in a demo from one that survives a year in production.

### Concrete example

```
Before: fact_sales has abc123 -> delivered_ts = NULL, updated_at = 2026-09-01
        MAX(updated_at) across the table = 2026-09-06

Production sets delivered_customer_date = 2026-09-07
DAG runs -> raw_orders refreshed
stg_orders recomputes GREATEST(...) -> updated_at = 2026-09-07

dbt run:
  bookmark = 2026-09-06
  WHERE updated_at > '2026-09-06'  -> finds abc123's line items
  MERGE on (abc123, 1) -> exists -> UPDATE in place

After: 2 rows touched instead of 112,650 rebuilt
```

Note: `fact_sales` does **not** carry `order_status`. What the merge actually rewrites is
`delivered_ts` (NULL -> a date) and `updated_at`.

An ordinary `WHERE purchase_date > bookmark` would **miss this entirely**, because the
order was purchased weeks ago.

---

## 12. Where Watermarking BREAKS

| # | Hole | What happens |
|---|---|---|
| 1 | **Change does not move `updated_at`** | Invisible forever |
| 2 | **In-flight transactions** | Row silently lost |
| 3 | **Exact ties** | Strict `>` drops it |
| 4 | **Deletes** | Cannot be found by definition |
| 5 | **Backfills with historical timestamps** | All land behind the bookmark |

### Hole 1 — a real bug in this project

`GREATEST` only reads **four columns, all from `orders`**. And `stg_order_items.sql` has
**no timestamp at all**:

```sql
SELECT order_id, order_item_id, product_id, seller_id,
       price, freight_value, (price + freight_value) AS total_item_value
FROM {{ source('olist', 'raw_order_items') }}
```

> Finance corrects a wrong price in `order_items` — 89.90 should have been 98.90.
> Nothing in `orders` changes. `GREATEST` returns the same value. `updated_at` does not
> move. **`fact_sales` keeps the wrong price forever**, and every revenue dashboard is
> quietly wrong until someone runs `--full-refresh`.

`fact_sales` pulls `price`, `freight_value` and `total_item_value` from items but
watermarks entirely on orders. **A fact is only as fresh as the one table its watermark
watches.**

### Hole 2 — in-flight transactions (the one that bites production teams)

```
10:00:00.0  Transaction A starts, writes a row stamped updated_at = 10:00:00
10:00:00.5  Pipeline reads: MAX(updated_at) = 10:00:01  (from other rows)
10:00:02    Transaction A finally COMMITS
```

That row was **stamped at 10:00:00 but not visible until 10:00:02.** The bookmark already
moved to 10:00:01. Next run asks for `> 10:00:01` — the row, sitting at 10:00:00, is now
**permanently invisible.** No error, no warning. Just a missing order.

### The fixes

| Fix | What it does | Cost |
|---|---|---|
| **Lookback window** | `> bookmark - INTERVAL '3 days'` | Slight extra work; MERGE dedupes. **Cheapest real fix.** |
| **`>=` instead of `>`** | Catches ties | Reprocesses boundary row; harmless with a unique key |
| **Periodic full refresh** | `--full-refresh` weekly | Catches everything, including deletes |
| **Watermark every source** | Add an items timestamp, `GREATEST` across both | Fixes Hole 1 properly |
| **CDC** | Read the write-ahead log | Real infra. **The actual production answer.** |

The one-line fix worth knowing (fixes holes 2, 3 and 5 at once):

```sql
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }}) - INTERVAL '3 days'
{% endif %}
```

---

## 13. Other Incremental Strategies

Two separate axes. Keeping them apart makes the landscape clear.

### Detection — how do you FIND what changed?

| Strategy | How | Catches updates? | Catches deletes? | Needs timestamp? |
|---|---|---|---|---|
| **Watermark** *(this project)* | `WHERE updated_at > MAX` | Yes | No | Yes |
| **Lookback window** | `> MAX - INTERVAL '3 days'` | Yes | No | Yes |
| **Sequence / ID** | `WHERE id > MAX(id)` | **No** | No | No |
| **Partition rebuild** | Fully rebuild last N days | Yes | Yes (in window) | No |
| **Hash comparison** | Hash the row, compare stored hash | Yes | No | **No** |
| **CDC** | Read the write-ahead log | Yes | **Yes** | No |

**Sequence/ID watermark** — `WHERE order_id > 45000`. Dead simple, completely wrong here.
IDs never change, so it finds *new* rows but is **blind to every update**. Fine for
immutable logs (page views, clicks); catastrophic for orders that change status.

**Hash comparison** — the direct answer to Hole 1. Hash each row's columns, store it,
rehash next run:

```sql
MD5(price || freight_value || product_id) AS row_hash
```

Hash differs -> the row changed. **No timestamp required.** Cost: you must read every row
to hash it, so you lose the "only scan recent rows" saving. Trades read efficiency for
correctness.

**Partition rebuild** — "always fully rebuild the last 7 days, never touch older."
Common with BigQuery/Spark. Catches deletes within the window and is easy to reason about.

### Write — how do you APPLY it? (dbt's `incremental_strategy`)

| Strategy | What it does | Use when |
|---|---|---|
| `append` | `INSERT` only, no dedupe | Immutable events — logs, clicks. Fast, but reprocessing duplicates. |
| `merge` | Upsert on `unique_key` | **This project's behaviour.** Rows mutate. |
| `delete+insert` | Delete matching keys, then insert | Warehouses with weak `MERGE` support |
| `insert_overwrite` | Replace whole partitions | Pairs with partition-rebuild detection |
| `microbatch` | Auto-splits into time-based batches (dbt 1.9+) | Big resumable backfills |

Set it explicitly:

```sql
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=[...]) }}
```

This project omits it and gets the Postgres default.

### dbt snapshots — the fix for "there is no timestamp"

```yaml
strategy: timestamp     # same watermark idea
updated_at: updated_at

strategy: check         # compares actual column VALUES
check_cols: ['price', 'order_status']
```

`check` needs **no timestamp at all** — it compares the columns you name against what it
stored last time. This is what would catch the silent price correction, **and** it gives
you SCD Type 2 history at the same time.

---

## 14. Where History Actually Lives

| Stage | Keeps history? | Why |
|---|---|---|
| Production -> S3 | **Yes** | New dated folder every run |
| S3 -> warehouse `raw_*` | **No** | Table dropped and rebuilt |
| `raw_*` -> `fact_sales` | **Partly** | Rows persist, but changes overwrite in place |

**S3 is the one place history survives.** That is the entire reason the lake exists.
Everything downstream is disposable; the lake is the archive.

```
orders/dt=2026-09-05/orders.parquet
orders/dt=2026-09-06/orders.parquet
orders/dt=2026-09-07/orders.parquet
```

**Production overwrites itself.** When an order goes shipped -> delivered, the row is
updated in place and the old value is gone. Production knows the *current* state of the
business, not its *history*.

**`fact_sales` is subtle** — rows accumulate and survive, but the merge **updates in
place**. The row lives on; the fact that it *used to say shipped* is gone.
So it has **row history, not change history**.

### SCD Type 2 — how you keep change history

Instead of updating a row, you **close it out and open a new one**:

| order_id | status | valid_from | valid_to | is_current |
|---|---|---|---|---|
| abc123 | shipped | 2026-09-01 | 2026-09-05 | false |
| abc123 | delivered | 2026-09-05 | *null* | **true** |

Now "what was this order's status on Sept 3rd?" has an answer.
**dbt has this built in** — a `snapshots/` folder, run with `dbt snapshot`.
**This project has none.** Adding it is ~20 lines and would be the single most impressive
addition.

---

## 15. What Rebuilds, and How

Per DAG run + `dbt run`:

| What | Who builds it | On a re-run |
|---|---|---|
| `raw_*` (9) | Airflow | **Full drop & replace** (`if_exists='replace'`) |
| `stg_*` (9) | dbt | `CREATE OR REPLACE VIEW` — redefined, costs nothing |
| `dim_*`, `fact_payments`, `fact_reviews` (5) | dbt | **Full drop & rebuild** |
| `fact_sales` (1) | dbt | **Incremental** — merges only changed rows |

**`fact_sales` is the ONLY thing in this project that does not rebuild from scratch.**

Caveat: `dbt run --full-refresh` overrides that and forces a drop+rebuild. The first build
needs the flag (the table must exist before `is_incremental()` can be true). Leave it in
permanently and you have silently disabled the one clever thing in the project.

### The saving is in WRITES, not READS

When `fact_sales` runs incrementally: it queries the view, the view scans all ~99k rows of
`raw_orders` fresh, the `WHERE` filters to 40, and only those 40 get merged.

---

## 16. Dates — Two Different Questions

### (a) Dates INSIDE the data
Olist covers roughly **September 2016 -> October 2018**, ~25 months of real orders.
**Fixed and historical. It never grows.** No live store is generating new orders.

### (b) `dt=` partitions in S3
Completely different number: **however many days you have run the DAG.**

### The bit that trips everyone up

Since the source data is frozen, **every partition contains the identical ~99,441 orders.**
`dt=2026-09-07` and `dt=2026-09-08` are byte-identical copies.

The daily partitioning is *structurally* correct — exactly what you would do in production
— but here it demonstrates the pattern rather than capturing real change.

This is why Phase 9 has you manually `UPDATE` the OLTP database before re-running. That
fake edit is the only way to make anything actually change, and it turns the incremental
logic from a claim into something you can watch execute.

**Honest framing:** *"the pipeline handles daily incremental loads"* is a statement about
the **architecture**, not about observed data.

Also note: re-running the DAG twice on the **same** day writes the same `dt=` key twice,
so the second run overwrites the first. Intentional (it makes re-runs safe), but it means
triggering 5 times today gives you one folder, not five.

---

## 17. This Project vs Real Companies

| | This project | Real companies |
|---|---|---|
| Extract | Full table scan | **CDC streaming** (Debezium / Fivetran / AWS DMS / Airbyte) |
| Lake format | Bare Parquet | **Iceberg / Delta Lake / Hudi** |
| Lake retention | Forever, no policy | Tiered storage, lifecycle rules, Glacier |
| Raw load | Drop & replace | `MERGE` for big tables |
| Change history | **None** | **SCD Type 2 / dbt snapshots** |
| Orchestration | Airflow (dbt run by hand) | Airflow / Dagster, dbt inside the DAG |
| Testing | 10 dbt tests, manual | Tests in CI, gating every merge |
| Scale | ~100k rows, one laptop | Billions of rows, cloud warehouse |

### CDC — the biggest single gap

Tools read the database's **write-ahead log** — the internal journal Postgres already keeps
of every change. You get a stream of "row X changed from this to that" in near-real-time,
and production barely notices because it is already writing that log anyway.
Catches updates **and deletes**, and needs no timestamp.

### Table formats

Iceberg / Delta / Hudi turn a pile of files into something that behaves like a real table:

- **Time travel** — literally `SELECT * FROM orders AS OF '2026-03-01'`
- **ACID transactions** — no half-written state if a job dies mid-write
- **Schema evolution** — add a column without rewriting history
- **Efficient updates/deletes** — needed for GDPR "delete this customer everywhere"

### Medallion architecture

Same idea as this project, different vocabulary:

| Medallion | This project |
|---|---|
| **Bronze** = raw, untouched | `raw_*` |
| **Silver** = cleaned, typed, deduplicated | `staging/` |
| **Gold** = business-ready star schema | `marts/` |

### Verdict

**Not the same as real life, but the difference is smaller than you would think.**
The *shape* here is correct and is genuinely what companies run:
source -> lake -> raw -> staging -> marts -> BI, orchestrated, contract-checked, tested.

What real companies add is mostly **scale** (CDC instead of full scans),
**durability** (table formats, retention), and **history** (SCD Type 2).

**The concepts transfer. The tools get swapped.**

---

## 18. Known Gaps In This Repo

Things where the README's pitch and the code disagree. Worth knowing before presenting it.

1. **Airflow does not run dbt.** The README diagram shows an arrow from Airflow to dbt,
   but there is no dbt task in the DAG. You run `dbt run` yourself from the terminal.
2. **The EL layer is full-refresh, not incremental.** `if_exists='replace'` drops and
   rebuilds each raw table every run. The incremental story is true **only inside dbt**.
3. **Only 1 of 9 tables has a contract.** The other 8 log a warning and pass unchecked.
4. **The contract is weaker than it looks.** `df.astype(str)` stringifies everything first,
   so every `"type": "string"` check passes automatically. Only the `enum` and `required`
   list do real work.
5. **`order_items` has no watermark** — silent price corrections are invisible (Hole 1).
6. **No lookback window** — vulnerable to in-flight transactions and ties.
7. **No delete handling.**
8. **No SCD Type 2 / snapshots** — no change history anywhere.
9. **Nothing is persisted** — no named volumes, so `docker compose down` wipes the
   warehouse and Metabase dashboards. Use `docker compose stop`.
10. **`pyarrow` and `jsonschema` are not in `requirements.txt`** — they work only because
    Airflow happens to depend on them.
11. **`geolocation` is loaded but unused** — ~1M rows moved every run for nothing.
12. **3 marts have zero tests** — `dim_products`, `fact_payments`, `fact_reviews`.

### Highest-value fixes

1. Add the **lookback window** to `fact_sales` (one line, prevents real data loss)
2. Add **dbt snapshots** on `stg_orders` (gives the project actual history)
3. Make **Airflow run dbt** (closes the biggest diagram-vs-code gap)
4. Test the **3 untested marts** (~20 minutes)
5. Declare **volumes** so `docker compose down` stops being destructive

---

## 19. Glossary

| Term | Meaning |
|---|---|
| **OLTP** | Online Transaction Processing — the production/website database |
| **OLAP** | Online Analytical Processing — the warehouse |
| **ELT** | Extract, Load, Transform — clean **after** loading |
| **ETL** | Extract, Transform, Load — clean **before** loading |
| **DAG** | Directed Acyclic Graph — a to-do list with ordering, no loops |
| **Parquet** | Compressed, column-oriented file format |
| **Hive-style partitioning** | Folder naming like `dt=2026-09-07/` |
| **Idempotent** | Running it twice gives the same result as running it once |
| **Data contract** | A formal agreement on data shape between producer and consumer |
| **Shift-left** | Catch quality problems at the door, not downstream |
| **Fact table** | The events you measure — long, skinny, numeric |
| **Dimension table** | The descriptions you slice by — short, wide, labels |
| **Grain** | What one row represents (here: one line item, not one order) |
| **Materialization** | How dbt persists a model: view, table, or incremental |
| **High-water mark** | The bookmark — `MAX(updated_at)` in the destination |
| **Upsert / MERGE** | Update if the key exists, insert if it does not |
| **CDC** | Change Data Capture — reading the DB's write-ahead log |
| **SCD Type 2** | Keeping change history by closing rows and opening new ones |
| **Lakehouse** | A query engine (Athena/Trino/Databricks) over object storage |
| **Medallion** | Bronze / Silver / Gold layering |

---

## 20. Misconceptions To Avoid

| Wrong | Right |
|---|---|
| "Data is cleaned before S3" | It is **validated**, never modified. Cleaning happens in dbt. |
| "The dbt source is S3" | It is the `raw_*` tables in Postgres. **dbt has no idea S3 exists.** |
| "Load is DB -> S3, extract is S3 -> warehouse" | Backwards. **Extract** = DB -> S3, **Load** = S3 -> warehouse. |
| "It uses Great Expectations" | It uses `jsonschema` with hand-written JSON Schema files. |
| "dbt sets `updated_at`" | dbt only **reads** it. Production writes it. |
| "Incremental means S3 keeps growing" | S3 accumulates **full snapshots**. Incremental lives only in `fact_sales`. |
| "Staging is appended to" | Staging is a **view**. It stores nothing. |
| "`fact_sales` appends new rows" | It **merges** (upserts). Appending would duplicate. |
| "dbt computes the data" | dbt generates SQL. **The database does all the work.** |
| "Sources are built by dbt" | Sources are **declarations**. `dbt run` builds 15 models, not 24. |
| "`updated_at` is a fixed position" | It is a **value that moves to NOW** when the row changes. |

### Two rules to memorise

1. **dbt reads timestamps, it does not create them.** No upstream signal -> no detection.
2. **Only `fact_sales` is incremental.** Everything else rebuilds completely, every run.
