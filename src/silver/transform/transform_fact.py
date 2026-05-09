from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.window import Window
from pyspark.sql.types import (
    ArrayType, StructType, StructField,
    StringType, IntegerType, DoubleType
)
from src.common.sql_functions import sk_hash_expr

# ── Line items JSON schema ─────────────────────────────────────────────
LINE_ITEM_SCHEMA = ArrayType(
    StructType([
        StructField("line_item_id", StringType()),
        StructField("product_id",   StringType()),
        StructField("quantity",     IntegerType()),
        StructField("price",        DoubleType()),
        StructField("status",       StringType()),
    ])
)


def transform_f_order_exploded(
    df:          DataFrame,   # ← raw_orders bronze
    d_customer:  DataFrame,   # ← silver d_customer
    d_product:   DataFrame,   # ← silver d_product
    d_date:      DataFrame,   # ← silver d_date
) -> DataFrame:

    # ── Step 1: hash customer_id FIRST, then drop raw ──────────────────
    df_with_hash = (
        df
        .withColumn(
            "customer_id_hashed",
            sk_hash_expr(["customer_id"])
        )
        .drop("customer_id")
        .withColumnRenamed("customer_id_hashed", "customer_id")
        .withColumn(
            "order_date_cast",
            F.col("order_date").cast("date")
        )
    )

    # ── Step 2: join d_customer — regular join (can be large) ──────────
    with_customer = (
        df_with_hash
        .join(
            d_customer.select("customer_id"),
            on  = "customer_id",
            how = "left",
        )
    )

    # ── Step 3: broadcast join d_date — small dim ──────────────────────
    with_date = (
        with_customer
        .join(
            F.broadcast(                            # ← broadcast hint
                d_date.select("date")
            ),
            on  = with_customer["order_date_cast"] == F.col("date"),
            how = "left",
        )
    )

    # ── Step 4: parse JSON once + compute order_value BEFORE explode ───
    df_with_order_value = (
        with_date
        .withColumn(
            "line_items_parsed",
            from_json(F.col("line_items"), LINE_ITEM_SCHEMA)
        )
        .withColumn(
            "order_value",
            F.round(
                F.aggregate(
                    F.col("line_items_parsed"),
                    F.lit(0.0).cast("double"),
                    lambda acc, x: acc + (
                        x["quantity"].cast("double") *
                        x["price"]
                    )
                ), 2
            )
        )
    )

    # ── Step 5: explode reuses already parsed array ────────────────────
    exploded = (
        df_with_order_value
        .withColumn(
            "line_item",
            explode(F.col("line_items_parsed"))
        )
    )

    # ── Step 6: extract + hash product_id BEFORE join ──────────────────
    exploded_with_hash = (
        exploded
        .withColumn(
            "product_id_raw",
            F.col("line_item.product_id")
        )
        .withColumn(
            "product_id_hashed",
            sk_hash_expr(["product_id_raw"])
        )
        .drop("product_id_raw")
    )

    # ── Step 7: broadcast join d_product — small dim ───────────────────
    with_product = (
        exploded_with_hash
        .join(
            F.broadcast(                            # ← broadcast hint
                d_product.select("product_id")
            ),
            on  = exploded_with_hash["product_id_hashed"] == d_product["product_id"],
            how = "left",
        )
        .drop("product_id_hashed")
    )

    # ── Step 8: select final intermediate columns ──────────────────────
    transform_exploded_expr: dict = {
        # ── order-level ─────────────────────────────────────────────────
        "order_id":         F.col("order_id").cast("string"),
        "customer_id":      F.col("customer_id"),
        "order_date_id":    F.col("date"),
        "order_status":     F.col("order_status").cast("string"),
        "order_value":      F.col("order_value"),
        # ── line-level ──────────────────────────────────────────────────
        "line_item_id":     F.col("line_item.line_item_id").cast("string"),
        "product_id":       F.col("product_id"),
        "quantity":         F.col("line_item.quantity").cast("integer"),
        "unit_price":       F.col("line_item.price").cast("decimal(10,2)"),
        "line_status":      F.col("line_item.status").cast("string"),
        "line_total_price": F.round(
                                F.col("line_item.quantity") *
                                F.col("line_item.price"), 2
                            ).cast("decimal(12,2)"),
        # ── audit ───────────────────────────────────────────────────────
        "created_ts":      F.col("_ingested_at").cast("timestamp"),

        "batch_id":         F.col("_batch_id").cast("string"),
    }

    return (
        with_product
        .withColumns(transform_exploded_expr)
        .select(*transform_exploded_expr.keys())
    )



def transform_f_order(df: DataFrame) -> DataFrame:
    """
    Input : f_order_exploded (intermediate)
    Grain : one row per order
    Logic : order_value already computed via F.aggregate
            just select + dedupe to order grain
    """
    transform_f_order_expr: dict = {
        "order_id":      F.col("order_id"),
        "customer_id":   F.col("customer_id"),
        "order_date_id": F.col("order_date_id"),
        "order_status":  F.col("order_status"),
        "order_value":   F.col("order_value"),  # ← pre-computed
        "created_ts":    F.col("created_ts"),
        "update_ts":     F.current_timestamp(),
    }

    return (
        df
        .withColumns(transform_f_order_expr)
        .select(*transform_f_order_expr.keys())
        .dropDuplicates(["order_id"])            # ← dedupe to order grain
    )


def transform_f_order_line(df: DataFrame) -> DataFrame:
    """
    Input : f_order_exploded (intermediate)
    Grain : one row per line item
    Logic : straight select — all columns already computed in intermediate
    """
    transform_f_order_line_expr: dict = {
        "line_sk":          sk_hash_expr(["line_item_id","order_id"])  ,
        "line_item_id":     F.col("line_item_id"),
        "order_id":         F.col("order_id"),
        "product_id":       F.col("product_id"),
        "order_date_id":    F.col("order_date_id"),
        "line_status":      F.col("line_status"),
        "quantity":         F.col("quantity"),
        "unit_price":       F.col("unit_price"),
        "line_total_price": F.col("line_total_price"),
        "created_ts":       F.col("created_ts"),
        "update_ts":        F.current_timestamp(),
    }

    return (
        df
        .withColumns(transform_f_order_line_expr)
        .select(*transform_f_order_line_expr.keys())
    )

