import os
from pathlib import Path

import psycopg2

conn_string = os.environ.get("DATABASE_URL")
if not conn_string:
    raise SystemExit("Set DATABASE_URL before running this script.")

# CSV files mapping to tables
csv_files = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}

data_dir = Path(__file__).resolve().parent / "data"

try:
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            for csv_file, table_name in csv_files.items():
                csv_path = data_dir / csv_file
                if not csv_path.exists():
                    raise FileNotFoundError(csv_path)

                print(f"Loading {csv_file} into {table_name}...")
                with csv_path.open("r", newline="", encoding="utf-8") as csv_data:
                    cursor.copy_expert(
                        f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                        csv_data,
                    )
                print(f"Loaded {csv_file}")

    print("All data loaded successfully.")
except Exception as exc:
    print(f"Error: {exc}")
    raise
