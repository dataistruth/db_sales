# Databricks notebook source
# ── Imports ────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from typing import Optional
import logging

# ── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Spark session (already available in Databricks notebook/job) ───────
spark = SparkSession.builder.getOrCreate()

# ── Config ─────────────────────────────────────────────────────────────



# ── Read values ────────────────────────────────────────────────────────
catalog       = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("silver_schema")
# ── Control table lives in silver ─────────────────────────────────────
control_table = f"{catalog}.{schema_silver}.optimize_control"  # ← same schema


# ── Setup control table ────────────────────────────────────────────────
def setup_control_table() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {control_table} (
            table_name    STRING,
            last_version  BIGINT,
            last_run_ts   TIMESTAMP
        ) USING DELTA
    """)
    logger.info(f"Control table ready: {control_table}")


# ── Get last optimized version ─────────────────────────────────────────
def get_last_optimized_version(table_name: str) -> int:
    result = spark.sql(f"""
        SELECT COALESCE(MAX(last_version), 0) AS v
        FROM {control_table}
        WHERE table_name = '{table_name}'
    """).collect()[0]["v"]
    return result


# ── Save optimized version ─────────────────────────────────────────────
def save_optimized_version(table_name: str, version: int) -> None:
    spark.sql(f"""
        MERGE INTO {control_table} AS t
        USING (
            SELECT
                '{table_name}'      AS table_name,
                {version}           AS last_version,
                CURRENT_TIMESTAMP() AS last_run_ts
        ) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT  *
    """)
    logger.info(f"Saved version {version} for {table_name}")


# ── Smart optimize ─────────────────────────────────────────────────────
def smart_optimize(table_name: str, zorder_col: str) -> None:

    logger.info(f"Starting smart_optimize for {table_name}")

    # ── Step 1: get last optimized version ────────────────────────────
    last_version = get_last_optimized_version(table_name)
    logger.info(f"Last optimized version: {last_version}")

    # ── Step 2: get current version ───────────────────────────────────
    current_version = spark.sql(f"""
        SELECT MAX(version) AS v
        FROM (DESCRIBE HISTORY {table_name})
    """).collect()[0]["v"]
    logger.info(f"Current version: {current_version}")

    if current_version == last_version:
        logger.info(f"No changes since last optimize — skipping ✅")
        return

    # ── Step 3: find touched partitions via CDF ───────────────────────
    changed_dates = (
        spark.read
        .format("delta")
        .option("readChangeFeed",  "true")
        .option("startingVersion", last_version + 1)
        .option("endingVersion",   current_version)
        .table(table_name)
        .select("order_date_id")
        .distinct()
        .collect()
    )

    dates = [str(row["order_date_id"]) for row in changed_dates]
    logger.info(f"Touched partitions: {dates}")

    if not dates:
        logger.info("No partitions touched — skipping ✅")
        save_optimized_version(table_name, current_version)
        return

    # ── Step 4: OPTIMIZE + ZORDER only touched partitions ─────────────
    for date in dates:
        logger.info(f"  Optimizing partition: {date}")
        spark.sql(f"""
            OPTIMIZE {table_name}
            WHERE order_date_id = '{date}'
            ZORDER BY ({zorder_col})
        """)

    # ── Step 5: save version ──────────────────────────────────────────
    save_optimized_version(table_name, current_version)
    logger.info(f"Done ✅ version {current_version} saved")


# ── Entry point ────────────────────────────────────────────────────────
if __name__ == "__main__":

    # ensure control table exists
    setup_control_table()

    # optimize both fact tables
    smart_optimize(
        table_name = f"{catalog}.{schema_silver}.f_order",
        zorder_col = "order_id",
    )
    smart_optimize(
        table_name = f"{catalog}.{schema_silver}.f_order_line",
        zorder_col = "order_id",
    )