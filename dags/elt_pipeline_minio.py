"""
ELT: Postgres OLTP -> MinIO (Parquet lake) -> Postgres analytics warehouse.

Task graph: one independent `extract_<table>_to_minio >> load_<table>_to_dw`
chain per table. 9 tables x 2 tasks = 18 tasks, no cross-table dependencies,
so they parallelize under the LocalExecutor.

Two design points worth being able to defend:

1. The load tasks read back *from the lake*, not from an XCom-passed DataFrame.
   That makes the lake the replay point: a warehouse rebuild never touches the
   production database again.

2. The object key is Hive-style -- <table>/dt=<YYYY-MM-DD>/<table>.parquet,
   where the date is Airflow's logical date. A re-run overwrites its own
   partition rather than duplicating data, and a single day can be backfilled
   in isolation.

Known limitation, deliberate: `load_from_minio_to_dw` uses if_exists='replace',
so the EL layer is full-refresh. The incremental story in this project lives
inside dbt (fact_sales), not here. See IMPLEMENTATION.md Phase 0, gap 2.
"""

from __future__ import annotations

import io
import json
import logging
import os

import pandas as pd
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jsonschema import ValidationError, validate
from minio import Minio

log = logging.getLogger(__name__)

TABLES = [
    "customers",
    "geolocation",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation",
]

CONTRACTS_DIR = os.path.join(os.path.dirname(__file__), "contracts")
BUCKET = os.getenv("MINIO_BUCKET", "staging-lake")


def get_minio_client() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def validate_data_contract(df: pd.DataFrame, table_name: str) -> None:
    """Interceptor between the source read and the lake write.

    Looks for contracts/<table>.json. Found -> validate, and raise ValueError
    on violation so the task fails loudly before bad data reaches the lake.
    Not found -> log a warning and pass the data through unvalidated.

    Only `orders` has a contract today, so 8 of 9 tables take the warning
    branch. Confirming that in the task logs is a step in Phase 4.
    """
    contract_path = os.path.join(CONTRACTS_DIR, f"{table_name}.json")

    try:
        with open(contract_path) as f:
            schema = json.load(f)
    except FileNotFoundError:
        log.warning("No contract found for %s. Skipping validation.", table_name)
        return

    records = df.astype(str).to_dict(orient="records")
    try:
        validate(instance=records, schema=schema)
    except ValidationError as e:
        raise ValueError(
            f"Data Contract violation in {table_name}: {e.message}"
        ) from e

    log.info("Data Contract validation passed for %s.", table_name)


def extract_to_minio(table_name: str, ds: str) -> str:
    """Read the whole table from OLTP, validate it, land it as Parquet."""
    hook = PostgresHook(postgres_conn_id="OLTP_DB")
    df = hook.get_pandas_df(f"SELECT * FROM {table_name};")
    log.info("Extracted %s rows from %s", len(df), table_name)

    validate_data_contract(df, table_name)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    size = buffer.tell()
    buffer.seek(0)

    client = get_minio_client()
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info("Created bucket %s", BUCKET)

    object_name = f"{table_name}/dt={ds}/{table_name}.parquet"
    client.put_object(BUCKET, object_name, buffer, length=size)
    log.info("Wrote s3://%s/%s (%s bytes)", BUCKET, object_name, size)

    return object_name


def load_from_minio_to_dw(table_name: str, ds: str) -> int:
    """Pull the partition back out of the lake and write it to the warehouse."""
    object_name = f"{table_name}/dt={ds}/{table_name}.parquet"

    client = get_minio_client()
    response = client.get_object(BUCKET, object_name)
    try:
        df = pd.read_parquet(io.BytesIO(response.read()))
    finally:
        response.close()
        response.release_conn()

    engine = PostgresHook(postgres_conn_id="DW_DB").get_sqlalchemy_engine()
    df.to_sql(
        name=f"raw_{table_name}",
        con=engine,
        schema="public",
        if_exists="replace",
        index=False,
    )
    log.info("Loaded %s rows into raw_%s", len(df), table_name)

    return len(df)


with DAG(
    dag_id="oltp_s3_olap_pipeline",
    description="Olist OLTP -> MinIO lake -> analytics warehouse",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_tasks=6,
    tags=["elt", "olist", "minio"],
    default_args={"retries": 1},
) as dag:

    for table in TABLES:
        extract_task = PythonOperator(
            task_id=f"extract_{table}_to_minio",
            python_callable=extract_to_minio,
            op_kwargs={"table_name": table, "ds": "{{ ds }}"},
        )

        load_task = PythonOperator(
            task_id=f"load_{table}_to_dw",
            python_callable=load_from_minio_to_dw,
            op_kwargs={"table_name": table, "ds": "{{ ds }}"},
        )

        extract_task >> load_task
