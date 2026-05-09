# Databricks notebook source
import dlt
import sys
from pyspark.sql.functions import col

sys.path.insert(0, spark.conf.get("bundle.sourcePath") )
from src.common.sql_functions import generate_date_dim

# ── Read bundle-injected config ──────────────────────────────────────
catalog       = spark.conf.get("bundle.catalog")
env           = spark.conf.get("bundle.env")
developer     = spark.conf.get("bundle.developer")
schema_bronze = spark.conf.get("bundle.schema.bronze")
schema_silver = spark.conf.get("bundle.schema.silver")
schema_gold   = spark.conf.get("bundle.schema.gold")
sourcePath    = spark.conf.get("bundle.sourcePath")

@dlt.table(name="d_date")
def d_date():
    return generate_date_dim(spark,"1900-01-01", "2099-12-31")