"""
generate_fact_data.py
=====================
Generates raw order fact data in JSONL format.
Each order record contains a JSON array of line items (product_id, quantity, price, status).
Loads customer_ids and product_ids from data/ref/dim_ids.json written by generate_dim_data.py
to guarantee strict referential integrity.

IMPORTANT: Run generate_dim_data.py first so dim_ids.json exists.

Usage (local):
    python generate_fact_data.py
    python generate_fact_data.py --orders 500
    python generate_fact_data.py --orders 200 --loops 3 --gap 10

Usage (Databricks notebook):
    - Widgets: orders, loops, gap, catalog, schema
    - Run as-is; IS_DATABRICKS auto-detected

Output per loop:
    Local     : data/raw/raw_orders/raw_orders_<loop>_<ts>.json
    Databricks: /Volumes/<catalog>/<schema>/raw_landing/raw_orders/raw_orders_<loop>_<ts>.json

Order payload structure:
    {
        "order_id":     "ORD-000001",
        "customer_id":  "C-00042",
        "order_date":   "2024-03-15",
        "order_status": "IN_PROGRESS",
        "line_items": [
            {"line_item_id": "LI-000001-1", "product_id": "P-0003", "quantity": 2, "price": 89.99, "status": "DELIVERED"},
            {"line_item_id": "LI-000001-2", "product_id": "P-0011", "quantity": 1, "price": 42.99, "status": "SHIPPED"}
        ],
        "_source_file": "generate_fact_data.py",
        "_ingested_at": "2024-03-15T10:00:00+00:00"
    }
"""

import json
import os
import sys
import random
import time
import argparse
import subprocess
from datetime import datetime, timezone, timedelta


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


# ---------------------------------------------------------------------------
# Faker — install if missing
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
    dbutils.widgets.text("orders",  "200",     "Orders per loop")       # noqa: F821
    dbutils.widgets.text("loops",   "1",       "Number of loops")       # noqa: F821
    dbutils.widgets.text("gap",     "30",      "Gap between loops (s)") # noqa: F821
    dbutils.widgets.text("catalog", "prod_db", "UC Catalog")            # noqa: F821
    dbutils.widgets.text("schema",  "bronze",  "UC Schema")             # noqa: F821

    class Args:
        orders  = int(dbutils.widgets.get("orders"))                     # noqa: F821
        loops   = int(dbutils.widgets.get("loops"))                      # noqa: F821
        gap     = int(dbutils.widgets.get("gap"))                        # noqa: F821
        catalog = dbutils.widgets.get("catalog")                         # noqa: F821
        schema  = dbutils.widgets.get("schema")                          # noqa: F821

    args = Args()
else:
    parser = argparse.ArgumentParser(description="Generate order fact raw data")
    parser.add_argument("--orders",  type=int, default=200, help="Number of orders per loop (default: 200)")
    parser.add_argument("--loops",   type=int, default=1,   help="Number of loops (default: 1)")
    parser.add_argument("--gap",     type=int, default=30,  help="Seconds between loops (default: 30)")
    args, _ = parser.parse_known_args()


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
if IS_DATABRICKS:
    BASE_DIR = f"/Volumes/db_sales/bronze/raw_landing"
    REF_PATH = f"{BASE_DIR}/ref/dim_ids.json"
else:
    BASE_DIR = "/Users/mukeshsingh/spark/db_sales/data/raw"
    REF_PATH = "/Users/mukeshsingh/spark/db_sales/data/ref/dim_ids.json"

SOURCE_FILE = "generate_fact_data.py"

print(f"IS_DATABRICKS : {IS_DATABRICKS}")
print(f"BASE_DIR      : {BASE_DIR}")
print(f"REF_PATH      : {REF_PATH}")
print(f"orders={args.orders}  loops={args.loops}  gap={args.gap}s\n")


# ---------------------------------------------------------------------------
# Load dim ID pools — strict referential integrity
# ---------------------------------------------------------------------------
if not os.path.exists(REF_PATH):
    raise FileNotFoundError(
        f"'{REF_PATH}' not found.\n"
        f"Run generate_dim_data.py first to create the ID pools."
    )

with open(REF_PATH) as f:
    dim_ids = json.load(f)

CUSTOMER_IDS = dim_ids["customer_ids"]
PRODUCT_IDS  = dim_ids["product_ids"]

print(f"  Loaded ID pools — customers={len(CUSTOMER_IDS)}  products={len(PRODUCT_IDS)}\n")


# ---------------------------------------------------------------------------
# Status definitions
# ---------------------------------------------------------------------------
LINE_STATUSES       = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]
LINE_STATUS_WEIGHTS = [15, 20, 55, 10]

ORDER_STATUSES      = ["OPEN", "IN_PROGRESS", "COMPLETED", "CANCELLED"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def audit() -> dict:
    return {
        "_source_file": SOURCE_FILE,
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def write_json(records: list, table_name: str, file_name: str) -> None:
    out_dir  = os.path.join(BASE_DIR, table_name)

    if IS_DATABRICKS:
        dbutils.fs.mkdirs(out_dir)          # noqa: F821  — UC Volume path
    else:
        os.makedirs(out_dir, exist_ok=True) # local filesystem

    out_path = os.path.join(out_dir, file_name)
    with open(out_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    print(f"  ✓  {out_path}  ({len(records)} records)")


def derive_order_status(line_items: list) -> str:
    """
    Derive order-level status from line item statuses.
    - ALL DELIVERED        → COMPLETED
    - ALL CANCELLED        → CANCELLED
    - ANY SHIPPED/PENDING  → IN_PROGRESS
    - ALL PENDING          → OPEN
    """
    statuses = {li["status"] for li in line_items}

    if statuses == {"DELIVERED"}:
        return "COMPLETED"
    elif statuses == {"CANCELLED"}:
        return "CANCELLED"
    elif "SHIPPED" in statuses or (
        "DELIVERED" in statuses and "PENDING" in statuses
    ):
        return "IN_PROGRESS"
    elif statuses == {"PENDING"}:
        return "OPEN"
    else:
        return "IN_PROGRESS"


def gen_line_items(order_num: int, num_lines: int) -> list:
    """
    Generate line items for an order.
    Strictly uses product_ids from PRODUCT_IDS pool.
    No duplicate products within same order.
    """
    selected_products = random.sample(
        PRODUCT_IDS,
        k=min(num_lines, len(PRODUCT_IDS))
    )

    line_items = []
    for idx, product_id in enumerate(selected_products, start=1):
        line_items.append({
            "line_item_id": f"LI-{str(order_num).zfill(6)}-{idx}",
            "product_id":   product_id,
            "quantity":     random.randint(1, 20),
            "price":        round(random.uniform(10.0, 400.0), 2),
            "status":       random.choices(
                                LINE_STATUSES,
                                weights=LINE_STATUS_WEIGHTS
                            )[0],
        })
    return line_items


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------
def gen_raw_orders(n: int, loop_num: int) -> list:
    """
    Generate n order records.
    - customer_id strictly from CUSTOMER_IDS pool
    - product_ids in line_items strictly from PRODUCT_IDS pool
    - order_status derived from line item statuses
    - order_id offset by loop to avoid collisions across loops
    """
    records   = []
    base_date = datetime(2024, 1, 1)
    id_offset = (loop_num - 1) * n

    for i in range(1, n + 1):
        order_num    = id_offset + i
        order_date   = base_date + timedelta(days=random.randint(0, 364))
        num_lines    = random.randint(1, 5)
        line_items   = gen_line_items(order_num, num_lines)
        order_status = derive_order_status(line_items)

        records.append({
            "order_id":     f"ORD-{str(order_num).zfill(6)}",
            "customer_id":  random.choice(CUSTOMER_IDS),
            "order_date":   order_date.strftime("%Y-%m-%d"),
            "order_status": order_status,
            "line_items":   json.dumps(line_items),
            **audit(),
        })

    return records


# ---------------------------------------------------------------------------
# Referential integrity check
# ---------------------------------------------------------------------------
def check_referential_integrity(orders: list) -> str:
    customer_set = set(CUSTOMER_IDS)
    product_set  = set(PRODUCT_IDS)
    broken       = []

    for order in orders:
        if order["customer_id"] not in customer_set:
            broken.append(f"order {order['order_id']} — unknown customer {order['customer_id']}")

        line_items = json.loads(order["line_items"])
        for li in line_items:
            if li["product_id"] not in product_set:
                broken.append(f"order {order['order_id']} — unknown product {li['product_id']}")

    return "PASS" if not broken else f"FAIL ({len(broken)} broken refs)"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":

    total_orders     = 0
    total_line_items = 0

    print("=" * 55)
    print(f"  Loops  : {args.loops}")
    print(f"  Orders : {args.orders} per loop")
    print(f"  Gap    : {args.gap}s between loops")
    print("=" * 55 + "\n")

    for loop in range(1, args.loops + 1):

        random.seed(loop * 13)
        Faker.seed(loop * 13)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"  Loop {loop}/{args.loops}  [{ts}]")

        orders = gen_raw_orders(args.orders, loop)

        write_json(
            orders,
            "raw_orders",
            f"raw_orders_{str(loop).zfill(2)}_{ts}.json"
        )

        loop_lines = sum(len(json.loads(o["line_items"])) for o in orders)
        ri_status  = check_referential_integrity(orders)
        print(f"    line items   : {loop_lines}")
        print(f"    RI check     : {ri_status}\n")

        total_orders     += len(orders)
        total_line_items += loop_lines

        if loop < args.loops:
            print(f"    Sleeping {args.gap}s before next loop...  (Ctrl+C to stop)\n")
            try:
                time.sleep(args.gap)
            except KeyboardInterrupt:
                print("\n  Interrupted. Exiting cleanly.")
                break

    print("=" * 55)
    print(f"  Done.")
    print(f"  Total orders     : {total_orders}")
    print(f"  Total line items : {total_line_items}")
    print("=" * 55 + "\n")