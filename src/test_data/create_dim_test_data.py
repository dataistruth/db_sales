"""
generate_dim_data.py
====================
Generates raw dimension data (customers, products) in JSON format.
Writes to local filesystem or Databricks UC Volume depending on environment.

Usage (local):
    python generate_dim_data.py
    python generate_dim_data.py --customers 200 --products 50

Usage (Databricks notebook):
    - Widgets: customers, products, catalog, schema
    - Run as-is; IS_DATABRICKS auto-detected

Output:
    Local     : data/raw/raw_customers/raw_customers_<ts>.json
                data/raw/raw_products/raw_products_<ts>.json
                data/ref/dim_ids.json   ← ID pools for fact generator

    Databricks: /Volumes/<catalog>/<schema>/raw_landing/raw_customers/...
                /Volumes/<catalog>/<schema>/raw_landing/raw_products/...
                /Volumes/<catalog>/<schema>/raw_landing/ref/dim_ids.json
"""

import json
import os
import sys
import argparse
import subprocess
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


# ---------------------------------------------------------------------------
# Faker — install if missing (Databricks serverless)
# ---------------------------------------------------------------------------
def ensure_faker():
    try:
        from faker import Faker  # noqa: F401
    except ImportError:
        if IS_DATABRICKS:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "faker"])
        else:
            raise RuntimeError("faker not installed. Run: pip install faker")


ensure_faker()
from faker import Faker  # noqa: E402

fake = Faker()

# ---------------------------------------------------------------------------
# Args / Widgets
# ---------------------------------------------------------------------------
if IS_DATABRICKS:
    dbutils.widgets.text("customers", "100", "Number of customers")  # noqa: F821
    dbutils.widgets.text("products", "50", "Number of products")  # noqa: F821
    dbutils.widgets.text("catalog", "prod_db", "UC Catalog")  # noqa: F821
    dbutils.widgets.text("schema", "bronze", "UC Schema")  # noqa: F821


    class Args:
        customers = int(dbutils.widgets.get("customers"))  # noqa: F821
        products = int(dbutils.widgets.get("products"))  # noqa: F821
        catalog = dbutils.widgets.get("catalog")  # noqa: F821
        schema = dbutils.widgets.get("schema")  # noqa: F821


    args = Args()
else:
    parser = argparse.ArgumentParser(description="Generate dimension raw data")
    parser.add_argument("--customers", type=int, default=100, help="Number of customers (default: 100)")
    parser.add_argument("--products", type=int, default=50, help="Number of products  (default: 50)")
    args, _ = parser.parse_known_args()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
if IS_DATABRICKS:
    BASE_DIR = f"/Volumes/db_sales/bronze/raw_landing"
    REF_DIR = f"{BASE_DIR}/ref"
else:
    BASE_DIR = "/Users/mukeshsingh/spark/db_sales/data/raw"
    REF_DIR = "/Users/mukeshsingh/spark/db_sales/data/ref"

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
SOURCE_FILE = "generate_dim_data.py"

print(f"IS_DATABRICKS : {IS_DATABRICKS}")
print(f"BASE_DIR      : {BASE_DIR}")
print(f"customers     : {args.customers}")
print(f"products      : {args.products}\n")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def audit() -> dict:
    return {
        "_source_file": SOURCE_FILE,
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def write_json(records: list, table_name: str, file_name: str) -> None:
    if IS_DATABRICKS:
        out_dir = f"{BASE_DIR}/{table_name}"
        out_path = f"{out_dir}/{file_name}"
        dbutils.fs.mkdirs(out_dir)  # noqa: F821
        content = "\n".join(json.dumps(rec) for rec in records)
        dbutils.fs.put(out_path, content, overwrite=True)  # noqa: F821
        print(f"  ✓  {out_path}  ({len(records)} records)")
    else:
        out_dir = os.path.join(BASE_DIR, table_name)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, file_name)
        with open(out_path, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")
        print(f"  ✓  {out_path}  ({len(records)} records)")


# ---------------------------------------------------------------------------
# Product master data
# ---------------------------------------------------------------------------
PRODUCT_CATALOG = [
    ("Yamazaki 12", "Whisky", 89.99),
    ("Yamazaki 18", "Whisky", 199.99),
    ("Hibiki Harmony", "Whisky", 119.99),
    ("Hibiki 21", "Whisky", 349.99),
    ("Toki Whisky", "Whisky", 49.99),
    ("Roku Gin", "Gin", 42.99),
    ("Haku Vodka", "Vodka", 38.99),
    ("Midori Melon", "Liqueur", 24.99),
    ("The Premium Malt's", "Beer", 18.99),
    ("Kinmugi", "Beer", 14.99),
    ("Bowmore 12", "Whisky", 64.99),
    ("Laphroaig 10", "Whisky", 59.99),
    ("Auchentoshan Three Wood", "Whisky", 74.99),
    ("Maker's Mark", "Bourbon", 34.99),
    ("Jim Beam Black", "Bourbon", 28.99),
    ("Courvoisier VSOP", "Cognac", 49.99),
    ("Martell Blue Swift", "Cognac", 54.99),
    ("Tres Generaciones", "Tequila", 44.99),
    ("Olmeca Altos Plata", "Tequila", 29.99),
    ("Malibu Original", "Liqueur", 19.99),
]


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def gen_raw_customers(n: int) -> list:
    records = []
    for i in range(1, n + 1):
        customer_id = f"C-{str(i).zfill(5)}"
        records.append({
            "customer_id": customer_id,
            "full_name": fake.name(),
            "email": fake.email(),
            "country": fake.country(),
            "created_ts": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(),
            **audit(),
        })
    return records


def gen_raw_products(n: int) -> list:
    records = []
    # Use fixed catalog first, then faker extras if n > catalog size
    catalog = PRODUCT_CATALOG[:n] if n <= len(PRODUCT_CATALOG) else PRODUCT_CATALOG

    for i, (name, category, price) in enumerate(catalog, start=1):
        records.append({
            "product_id": f"P-{str(i).zfill(4)}",
            "product_name": name,
            "category": category,
            "unit_price": str(price),
            "created_ts": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(),
            **audit(),
        })

    # Fill remaining with faker-generated products if n > catalog size
    for i in range(len(catalog) + 1, n + 1):
        records.append({
            "product_id": f"P-{str(i).zfill(4)}",
            "product_name": fake.catch_phrase(),
            "category": fake.random_element(["Whisky", "Gin", "Vodka", "Rum", "Tequila"]),
            "unit_price": str(round(fake.pyfloat(min_value=10, max_value=400, right_digits=2), 2)),
            "created_ts": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(),
            **audit(),
        })

    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":

    print("=" * 55)
    print("  Generating dimension data")
    print("=" * 55)

    # Generate
    customers = gen_raw_customers(args.customers)
    products = gen_raw_products(args.products)

    # Write JSONL files
    write_json(customers, "raw_customers", f"raw_customers_{TIMESTAMP}.json")
    write_json(products, "raw_products", f"raw_products_{TIMESTAMP}.json")

    # Write ref/dim_ids.json — used by fact generator for referential integrity
    dim_ids = {
        "customer_ids": [r["customer_id"] for r in customers],
        "product_ids": [r["product_id"] for r in products],
    }

    if IS_DATABRICKS:
        dbutils.fs.mkdirs(REF_DIR)  # noqa: F821
        ref_path = f"{REF_DIR}/dim_ids.json"
        dbutils.fs.put(ref_path, json.dumps(dim_ids, indent=2), overwrite=True)  # noqa: F821
        print(f"\n  ✓  {ref_path}  (ID pools saved for fact generator)")
    else:
        os.makedirs(REF_DIR, exist_ok=True)
        ref_path = os.path.join(REF_DIR, "dim_ids.json")
        with open(ref_path, "w") as f:
            json.dump(dim_ids, f, indent=2)
        print(f"\n  ✓  {ref_path}  (ID pools saved for fact generator)")

    print(f"\n{'=' * 55}")
    print(f"  Done.")
    print(f"  customers : {len(customers)}")
    print(f"  products  : {len(products)}")
    print(f"{'=' * 55}\n")
