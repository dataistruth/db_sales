from pyspark.sql import DataFrame
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def sk_hash_expr(primary_col_list: List[str]) -> F.Column:
    """
    Returns an MD5 Column expression for the given natural key columns.
    Use directly inside withColumn().
    """
    if not primary_col_list:
        raise ValueError("primary_col_list cannot be empty")

    return F.md5(
        F.concat_ws("|",
            *[F.coalesce(F.col(c), F.lit("NULL")) for c in primary_col_list]
        )
    )


def generate_date_dim(spark: SparkSession,start_date: str, end_date: str):
    return (
        spark.sql(f"""
        SELECT
            explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date
        """)
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("day_of_week", F.date_format("date", "E"))
        .withColumn("is_weekend", F.col("day_of_week").isin("Sat", "Sun"))
    )