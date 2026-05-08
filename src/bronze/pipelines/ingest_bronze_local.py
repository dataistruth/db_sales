
import os
import json
from pyspark.sql import SparkSession

env = os.getenv("ENV", "local")

if env == "databricks":
    spark = SparkSession.getActiveSession()
else:
    base_path="/Users/mukeshsingh/spark/db_sales/data/raw"
    out_base_path="/Users/mukeshsingh/spark/db_sales/data/silver"
    chk_base_path="/Users/mukeshsingh/spark/db_sales/data/checkpoint/silver"
    spark = (
        SparkSession.builder
        .appName("bronze-ingestion")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
print(f"current working directory is {os.getcwd()}")
print(f"Env is {env}")

config_path="../../../config/bronze_tables.json"

with open(config_path) as f:
    config = json.load(f)

print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
active_tables = [t for t in config if t["is_active"]]


active_tables = [t for t in config if t["is_active"]]
queries = []  # collect all stream handles

for t in active_tables:
    table_nm = t["table_nm"]
    path     = f"{base_path}/{table_nm}"
    out_path = f"{out_base_path}/{table_nm}"
    chk_path = f"{chk_base_path}/{table_nm}"

    schema = spark.read.json(str(path)).schema
    print(f"Starting stream for {table_nm}")
    print(f"  src : {path}")
    print(f"  sink: {out_path}")
    print(f"  chk : {chk_path}")

    df = (
        spark.readStream
        .schema(schema)
        .json(str(path))
    )

    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", str(chk_path))
        .trigger(processingTime="30 seconds")
        .start(str(out_path))
    )

    queries.append(query)
# All streams are now running concurrently
print(f"\n{len(queries)} stream(s) running:")
for q in queries:
    print(f"  id={q.id}  name={q.name}  status={q.status['message']}")

# Block until ANY stream terminates (or fails)
spark.streams.awaitAnyTermination()