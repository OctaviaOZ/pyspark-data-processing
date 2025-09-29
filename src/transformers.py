"""
This module contains the core data transformation logic for the Spark pipeline.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def unify_actions(
    clicks: DataFrame, add_to_carts: DataFrame, previous_orders: DataFrame
) -> DataFrame:
    """Unifies the different user action dataframes into a single dataframe."""
    clicks_df = clicks.select(
        "customer_id",
        "item_id",
        F.lit(1).alias("action_type"),
        F.col("click_time").alias("timestamp"),
        F.to_date(F.regexp_replace("dt", "dt=", "")).alias("dt"),
    )
    add_to_carts_df = add_to_carts.select(
        "customer_id",
        F.col("config_id").alias("item_id"),
        F.lit(2).alias("action_type"),
        F.col("occurred_at").alias("timestamp"),
        F.to_date(F.regexp_replace("dt", "dt=", "")).alias("dt"),
    )
    previous_orders_df = previous_orders.select(
        "customer_id",
        F.col("config_id").alias("item_id"),
        F.lit(3).alias("action_type"),
        F.col("order_date").alias("timestamp"),
        F.col("order_date").alias("dt"),
    )
    return clicks_df.unionByName(add_to_carts_df).unionByName(previous_orders_df)

def get_customer_actions(
    actions: DataFrame,
    process_date: str,
    action_history_length: int,
    salt_buckets: int,
) -> DataFrame:
    """For each customer, get the last N actions before the process_date, mitigating data skew with salting."""
    actions_before_process_date = actions.filter(
        F.col("dt") < F.to_date(F.lit(process_date), "yyyy-MM-dd")
    )

    # Stage 1: Distribute and aggregate with salt
    salted_actions = actions_before_process_date.withColumn("salt", (F.rand() * salt_buckets).cast("int"))
    window_spec = Window.partitionBy("customer_id", "salt").orderBy(F.col("timestamp").desc())

    customer_actions_salted = (
        salted_actions.withColumn("action", F.struct("timestamp", "item_id", "action_type"))
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= action_history_length)
        .groupBy("customer_id", "salt")
        .agg(F.collect_list("action").alias("actions"))
    )

    # Stage 2: Merge salted results
    customer_actions_merged = (
        customer_actions_salted.groupBy("customer_id")
        .agg(F.flatten(F.collect_list("actions")).alias("actions"))
        .withColumn("actions", F.sort_array(F.col("actions"), asc=False))
        .withColumn("actions", F.slice(F.col("actions"), 1, action_history_length))
    )

    # Pad the results
    padding_array = F.array([
        F.struct(F.lit(0).alias("item_id"), F.lit(0).alias("action_type"))
    ] * action_history_length)

    customer_actions_padded = customer_actions_merged.withColumn(
        "actions",
        F.slice(
            F.concat(F.col("actions"), padding_array), 1, action_history_length
        ),
    )

    return customer_actions_padded.select(
        "customer_id",
        F.col("actions.item_id").alias("actions"),
        F.col("actions.action_type").alias("action_types"),
    )

def get_impressions_for_date(impressions: DataFrame, process_date: str) -> DataFrame:
    """Get the impressions for a specific date and explode them."""
    return (
        impressions.filter(F.col("dt") == f"dt={process_date}")
        .select("customer_id", F.explode("impressions").alias("impression"))
        .select(
            "customer_id",
            F.col("impression.item_id").alias("item_id"),
            F.col("impression.is_order").alias("is_order"),
        )
    )