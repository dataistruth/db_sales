# Databricks notebook source
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

env = os.getenv("ENV", "local")
print(f"current working directory is {os.getcwd()}")
print(f"Env is {env}")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
BASE_RAW_LOC = dbutils.widgets.get("raw_loc")
CONFIG_PATH = dbutils.widgets.get("config_path")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

print("=== RUNTIME CONFIG ===")
print("catalog      :", CATALOG)
print("bronze_schema:", BRONZE_SCHEMA)
print("raw_loc      :", BASE_RAW_LOC)
print("config_path  :", CONFIG_PATH)
print("current_user :", spark.sql("SELECT current_user()").collect()[0][0])
print("current_cat  :", spark.sql("SELECT current_catalog()").collect()[0][0])

# Test volume access
try:
    files = dbutils.fs.ls(BASE_RAW_LOC)
    print("Volume access : OK —", len(files), "items found")
except Exception as e:
    print("Volume access : FAILED →", str(e))

with open(CONFIG_PATH) as f:
    config = json.load(f)

active_tables = [t for t in config if t["is_active"]]



active_tables = [t for t in config if t["is_active"]]
queries = []  # collect all stream handles

for t in active_tables:
    table_nm = t["table_nm"]
    target_table=f"{CATALOG}.bronze.{table_nm}"
    path     = f"{BASE_RAW_LOC}/{table_nm}"



    path = f"{BASE_RAW_LOC}/{table_nm}"
    chk_path = f"{BASE_RAW_LOC}/checkpoint/{table_nm}"
    schema_path = f"{BASE_RAW_LOC}/schema/{table_nm}"


    print(f"  src : {path}")
    print(f"  sink: {target_table}")
    print(f"  chk : {chk_path}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(path)
        .withColumn("_source_file_path", F.col("_metadata.file_path"))  # full volume path of source file
        .withColumn("_source_file_name", F.col("_metadata.file_name"))  # just the filename
        .withColumn("_source_file_size", F.col("_metadata.file_size"))  # bytes
        .withColumn("_source_modified_at", F.col("_metadata.file_modification_time"))  # when file landed
        .withColumn("_ingested_at", F.current_timestamp())  # when spark processed it
        .withColumn("_batch_id", F.col("_metadata.file_path"))  # or pass in as widget
        .withColumn("_pipeline_name", F.lit(table_nm))

    )

    write = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", chk_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
    )

    query = write.toTable(target_table)
    queries.append(query)
# All streams are now running concurrently
print(f"\n{len(queries)} stream(s) running:")
for q in queries:
    print(f"  id={q.id}  name={q.name}  status={q.status['message']}")

