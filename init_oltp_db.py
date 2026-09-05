"""Seed the simulated production database from the Kaggle CSVs.

Runs on the HOST, not in a container -- it connects to localhost:5433, the
published port of postgres-oltp. Requires the 9 CSVs in raw_data/ (Phase 1)
and the containers up (Phase 2).

    uv run python init_oltp_db.py

Safely re-runnable: every table is written with if_exists="replace".
"""

from __future__ import annotations

import os
import sys

import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.getenv(
    "OLTP_DB_URL",
    "postgresql+psycopg2://admin:admin_password@localhost:5433/olist_production",
)

RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw_data")

# The keys are the exact filenames the Kaggle archive ships. A renamed file is
# not an error -- it is skipped with a warning, and only surfaces as a dbt
# failure three phases later. Match them exactly.
csv_to_table_mapping = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation",
}


def main() -> int:
    engine = create_engine(DB_URL)

    loaded, skipped = 0, 0
    for filename, table_name in csv_to_table_mapping.items():
        path = os.path.join(RAW_DATA_DIR, filename)

        if not os.path.exists(path):
            print(f"WARNING: File {path} not found. Skipping {table_name}.")
            skipped += 1
            continue

        print(f"Loading {filename} -> {table_name} ...", end=" ", flush=True)
        df = pd.read_csv(path)
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"{len(df):,} rows")
        loaded += 1

    print(f"\nDone. {loaded} tables loaded, {skipped} skipped.")
    return 1 if skipped else 0


if __name__ == "__main__":
    sys.exit(main())
