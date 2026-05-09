from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from src.common.sql_functions import sk_hash_expr

def transform_d_customer(df: DataFrame) -> DataFrame:
    # ── column expression map ──────────────────────────────────────────────────────
    transform_d_customer_expr: dict = {
        "customer_id":  sk_hash_expr(["customer_id"]),
        "full_name":    F.col("full_name").cast("string"),
        "email":        F.col("email").cast("string"),
        "country":      F.col("country").cast("string"),
        "created_ts":   F.col("created_ts").cast("timestamp"),
        "update_ts":    F.current_timestamp(),
    }

    # ── select derived from dict keys — single source of truth ────────────────────
    d_customer_df = (
        df
        .withColumns(transform_d_customer_expr)
        .select(*transform_d_customer_expr.keys())
    )

    df_uniq = d_customer_df.dropDuplicates(["customer_id"])
    return df_uniq

def transform_d_product(df: DataFrame) -> DataFrame:
    # ── column expression map ──────────────────────────────────────────────────────
    transform_d_product_expr: dict = {
        "product_id":   sk_hash_expr(["product_id"]),  # ← derived SK as PK
        "product_name": F.col("product_name").cast("string"),
        "category":     F.col("category").cast("string"),
        "unit_price":   F.col("unit_price").cast("decimal(10,2)"),   # ← cast from string
        "price_tier":   F.when(F.col("unit_price").cast("decimal(10,2)") < 50,  "BUDGET")
                         .when(F.col("unit_price").cast("decimal(10,2)") < 100, "MID")
                         .otherwise("PREMIUM"),
        "created_ts":   F.col("created_ts").cast("timestamp"),
        "update_ts":    F.current_timestamp(),
    }

    # ── select derived from dict keys — single source of truth ────────────────────
    d_product_df = (
        df
        .withColumns(transform_d_product_expr)
        .select(*transform_d_product_expr.keys())
    )

    df_uniq = d_product_df.dropDuplicates(["product_id"])
    return df_uniq

