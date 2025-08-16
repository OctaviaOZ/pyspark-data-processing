"""
This module orchestrates the data pipeline by combining the reading,
transforming, and writing steps.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transformers import (
    unify_actions,
    get_customer_actions,
    get_impressions_for_date,
    ACTION_HISTORY_LENGTH
)

def create_training_data(
    impressions: DataFrame,
    clicks: DataFrame,
    add_to_carts: DataFrame,
    previous_orders: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    Creates the training data for the model by executing the end-to-end pipeline.
    """
    actions = unify_actions(clicks, add_to_carts, previous_orders).cache()

    customer_actions = get_customer_actions(actions, process_date)
    impressions_on_date = get_impressions_for_date(impressions, process_date)

    training_df = impressions_on_date.join(
        customer_actions, "customer_id", "left"
    )

    zero_array = F.array([F.lit(0)] * ACTION_HISTORY_LENGTH)
    training_df = training_df.withColumn(
        "actions", F.coalesce(F.col("actions"), zero_array)
    ).withColumn(
        "action_types", F.coalesce(F.col("action_types"), zero_array)
    )
    
    return training_df