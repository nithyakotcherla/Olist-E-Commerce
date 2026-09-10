import io
import json
import os
from datetime import datetime

import boto3
import jsonschema
import pandas as pd
from jsonschema import validate
from sqlalchemy import text

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration
TABLES = ['customers', 'orders', 'order_items', 'products', 'sellers', 'geolocation', 'order_payments', 'order_reviews', 'product_category_name_translation']
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'staging-lake'
DBT_DIR = '/opt/airflow/dbt_ecommerce'
DBT_BIN = '/opt/airflow/dbt_venv/bin/dbt'

# Validate a random sample rather than all ~100k records in one call.
# The full-table validate() was the slowest step in the extract task.
CONTRACT_SAMPLE_SIZE = 2000


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )


def _json_safe(df):
    """Convert a DataFrame to JSON-serializable records WITHOUT stringifying everything.

    The previous implementation called df.astype(str), which made every value a
    string and caused every {"type": "string"} assertion to pass trivially. Here
    only timestamps are stringified; numerics stay numeric so the contracts'
    integer/number/minimum assertions do real work. NaN/NaT become None so the
    ["string", "null"] unions match.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    out = out.astype(object).where(pd.notnull(out), None)
    return out.to_dict(orient='records')


def validate_data_contract(df, table_name):
    """Loads the JSON schema contract and validates a sample of the extracted dataframe."""
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    contract_path = os.path.join(dag_dir, 'contracts', f'{table_name}.json')

    try:
        with open(contract_path, 'r') as file:
            contract_schema = json.load(file)
    except FileNotFoundError:
        print(f"⚠️ No contract found for {table_name} at {contract_path}. Bypassing validation.")
        return

    sample = df if len(df) <= CONTRACT_SAMPLE_SIZE else df.sample(CONTRACT_SAMPLE_SIZE, random_state=42)
    records = _json_safe(sample)

    try:
        validate(instance=records, schema=contract_schema)
    except jsonschema.exceptions.ValidationError as e:
        raise ValueError(f"❌ DATA CONTRACT VIOLATION in {table_name}: {e.message}")

    print(f"✅ Data Contract validation passed for {table_name} "
          f"({len(sample):,} of {len(df):,} records sampled).")


def extract_to_minio(table_name, execution_date):
    """Extracts data from OLTP, validates contract, and uploads to MinIO partitioned by date."""
    # 1. Extract from Postgres OLTP
    src_hook = PostgresHook(postgres_conn_id='OLTP_DB')
    df = src_hook.get_pandas_df(f"SELECT * FROM {table_name};")

    # ---------------------------------------------------------
    # The Data Contract Interceptor
    # ---------------------------------------------------------
    validate_data_contract(df, table_name)

    # 2. Convert to Parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # 3. Upload to MinIO using Hive-style partitioning (dt=YYYY-MM-DD)
    s3_client = get_s3_client()

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)

    partitioned_key = f"{table_name}/dt={execution_date}/{table_name}.parquet"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=partitioned_key,
        Body=parquet_buffer.getvalue()
    )
    print(f"Uploaded {table_name} ({len(df):,} rows) to MinIO at {partitioned_key}")


def load_from_minio_to_dw(table_name, execution_date):
    """Downloads partitioned Parquet from MinIO and loads into DW."""
    s3_client = get_s3_client()

    partitioned_key = f"{table_name}/dt={execution_date}/{table_name}.parquet"

    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=partitioned_key)
    parquet_data = response['Body'].read()

    df = pd.read_parquet(io.BytesIO(parquet_data))

    dest_hook = PostgresHook(postgres_conn_id='DW_DB')
    engine = dest_hook.get_sqlalchemy_engine()

    target = f"raw_{table_name}"

    # A plain if_exists='replace' issues DROP TABLE, which Postgres refuses once
    # dbt has built staging views on top of these tables:
    #   "cannot drop table raw_orders because other objects depend on it"
    # TRUNCATE + append gives the same full-refresh semantics without breaking
    # dependent objects. If the column layout has genuinely changed, fall back to
    # a CASCADE drop and let the downstream dbt run rebuild the views.
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT to_regclass(:t)"), {"t": f"public.{target}"}
        ).scalar() is not None

        if exists:
            cols = {r[0] for r in conn.execute(
                text("SELECT column_name FROM information_schema.columns "
                     "WHERE table_schema='public' AND table_name=:t"), {"t": target}
            )}
            if cols == set(df.columns):
                conn.execute(text(f'TRUNCATE TABLE public."{target}"'))
                mode = 'append'
            else:
                print(f"Schema drift detected on {target}; rebuilding with CASCADE.")
                conn.execute(text(f'DROP TABLE IF EXISTS public."{target}" CASCADE'))
                mode = 'replace'
        else:
            mode = 'replace'

    df.to_sql(
        name=target,
        con=engine,
        schema='public',
        if_exists=mode,
        index=False,
        method='multi',
        chunksize=1000
    )
    print(f"Loaded {table_name} ({len(df):,} rows, mode={mode}) from {partitioned_key} to Data Warehouse")


# Define the DAG
with DAG(
    dag_id='oltp_s3_olap_pipeline',
    description='Extract from OLTP, load to S3 partitioned, load to DW, then transform with dbt',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_tasks=4,
) as dag:

    load_tasks = []

    for table in TABLES:
        extract_task = PythonOperator(
            task_id=f'extract_{table}_to_minio',
            python_callable=extract_to_minio,
            op_kwargs={'table_name': table, 'execution_date': '{{ ds }}'}
        )

        load_task = PythonOperator(
            task_id=f'load_{table}_to_dw',
            python_callable=load_from_minio_to_dw,
            op_kwargs={'table_name': table, 'execution_date': '{{ ds }}'}
        )

        # Set task dependencies: Extract must finish before Load
        extract_task >> load_task
        load_tasks.append(load_task)

    # ---------------------------------------------------------
    # Transformation layer - closes the README's "Airflow triggers dbt" gap.
    # Every load must land before dbt models the star schema.
    # ---------------------------------------------------------
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --profiles-dir {DBT_DIR}',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} test --profiles-dir {DBT_DIR}',
    )

    load_tasks >> dbt_run >> dbt_test
