"""
This module defines the schemas for the input dataframes.

Defining explicit schemas is a critical performance optimization for Spark.
It avoids the need for Spark to infer the schema, which is a slow operation,
and it prevents incorrect data types from being assigned.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
    DateType,
    ArrayType,
)

# --- Impressions Schema ---
# The schema for the daily impression data.
impressions_schema = StructType(
    [
        # dt is kept as StringType to handle the raw Hive partition format (e.g., "dt=2025-08-16").
        StructField("dt", StringType(), nullable=False),
        StructField("ranking_id", StringType(), nullable=True),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField(
            "impressions",
            ArrayType(
                StructType(
                    [
                        StructField("item_id", IntegerType(), nullable=False),
                        StructField("is_order", BooleanType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)

# --- Clicks Schema ---
# The schema for user click events.
clicks_schema = StructType(
    [
        StructField("dt", StringType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("item_id", IntegerType(), nullable=False),
        StructField("click_time", TimestampType(), nullable=True),
    ]
)

# --- Add to Carts Schema ---
# The schema for user add-to-cart events.
add_to_carts_schema = StructType(
    [
        StructField("dt", StringType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("config_id", IntegerType(), nullable=False),
        # simple_id represents the specific size of the item and is not used in this pipeline.
        StructField("simple_id", IntegerType(), nullable=True),
        StructField("occurred_at", TimestampType(), nullable=True),
    ]
)

# --- Previous Orders Schema ---
# The schema for historical order data.
previous_orders_schema = StructType(
    [
        # Note: This schema does not contain simple_id as it's not required for the final model features.
        StructField("order_date", DateType(), nullable=True),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("config_id", IntegerType(), nullable=False),
    ]
)