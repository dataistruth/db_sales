# Databricks notebook source
import dlt
import sys
from pyspark.sql import functions as F
sys.path.insert(0, spark.conf.get("bundle.sourcePath") )
from src.silver.transform.transform_dim import  ( transform_d_customer ,
                                                  transform_d_product
                                                  )

from src.silver.transform.transform_fact import (transform_f_order_exploded,
                                                 transform_f_order,
                                                 transform_f_order_line )

from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable



# ── Read bundle-injected config ──────────────────────────────────────
catalog       = spark.conf.get("bundle.catalog")
env           = spark.conf.get("bundle.env")
developer     = spark.conf.get("bundle.developer")
schema_bronze = spark.conf.get("bundle.schema.bronze")
schema_silver = spark.conf.get("bundle.schema.silver")
schema_gold   = spark.conf.get("bundle.schema.gold")
sourcePath    = spark.conf.get("bundle.sourcePath")




# ── Step 1: Declare target table explicitly ────────────────────────────
dlt.create_streaming_table(
    name    = "d_customer",
    comment = "SCD Type 1 drug dimension",
    expect_all = {
        "email_not_null": "email IS NOT NULL"
        }
)

# ── Step 2: Streaming view as source ──────────────────────────────────
@dlt.view(name="d_customer_staged")
def d_customer_staged():
    bronze_table = f"{catalog}.{schema_bronze}.raw_customers"
    return (
        dlt.read_stream(bronze_table)
        .transform(transform_d_customer)
    )

# ── Step 3: SCD Type 1 ────────────────────────────────────────────────
dlt.apply_changes(
    target             = "d_customer",
    source             = "d_customer_staged",
    keys               = ["customer_id"],
    sequence_by        = col("created_ts"),
    stored_as_scd_type = 2,
    except_column_list=["update_ts"]
)
###New table - d_product

dlt.create_streaming_table(
    name="d_product",
    comment="This is silver product table ",
    expect_all ={
        "product_id_not_null" : "product_id IS NOT NULL"
    }
)

@dlt.view(name="d_product_staged")
def d_product_staged():
    bronze_table = f"{catalog}.{schema_bronze}.raw_products"
    return (
        dlt.read_stream(bronze_table)
        .transform(transform_d_product)

    )

dlt.apply_changes(
    target="d_product",
    source="d_product_staged",
    keys=["product_id"],
    sequence_by=col("created_ts"),
    stored_as_scd_type=1,
    except_column_list=["update_ts"]
)

# ── Intermediate — temporary, streaming ───────────────────────────────
@dlt.table(
    name      = "f_order_exploded",
    comment   = "Intermediate: dims joined before explode",
    temporary = True
)
def f_order_exploded():
    bronze_table = f"{catalog}.{schema_bronze}.raw_orders"
    return transform_f_order_exploded(
        df         = spark.readStream.table(bronze_table),  # ← external UC table
        d_customer = dlt.read("d_customer"),
        d_product  = dlt.read("d_product"),
        d_date     = dlt.read("d_date"),
    )


# ── f_order — simple dedupe, order_value pre-computed ─────────────────
dlt.create_streaming_table(
    name      = "f_order",
    comment   = "One row per order with aggregated order_value",
    partition_cols=["order_date_id"],  # ← partition not cluster_by
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    expect_all = {
        "order_id_not_null":    "order_id IS NOT NULL",
        "order_value_not_null": "order_value IS NOT NULL",
    }
)

@dlt.view(name="f_order_staged")
def f_order_staged():
    return (
        dlt.read_stream("f_order_exploded")
        .transform(transform_f_order)           # ← just select + dedupe
    )

dlt.apply_changes(
    target             = "f_order",
    source             = "f_order_staged",
    keys               = ["order_id","order_date_id"],
    sequence_by        = col("created_ts"),
    stored_as_scd_type = 1,
    except_column_list=["update_ts"]
)

# Line table
dlt.create_streaming_table(
    name="f_order_line",
    comment = "Stores the record at line level",
    partition_cols=["order_date_id"],  # ← partition not cluster_by
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

@dlt.view(name="f_order_line_staged")
def f_order_line_staged():
    return(
        dlt.read_stream("f_order_exploded")
        .transform(transform_f_order_line)
    )

dlt.apply_changes(
    target="f_order_line",
    source="f_order_line_staged",
    keys=["line_sk","order_date_id"],
    sequence_by = col("order_date_id",),
    stored_as_scd_type =2 ,
    except_column_list=["update_ts"]
)

