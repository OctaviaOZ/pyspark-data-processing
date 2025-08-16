"""
This module contains functions for reading the raw data sources for the pipeline.
"""
from pyspark.sql import SparkSession, DataFrame

from src.schemas import (
    impressions_schema,
    clicks_schema,
    add_to_carts_schema,
    previous_orders_schema,
)

def read_impressions(spark: SparkSession, path: str) -> DataFrame:
    """Reads the impressions data from a JSON source."""
    return spark.read.schema(impressions_schema).json(path)

def read_clicks(spark: SparkSession, path: str) -> DataFrame:
    """Reads the clicks data from a JSON source."""
    return spark.read.schema(clicks_schema).json(path)

def read_add_to_carts(spark: SparkSession, path: str) -> DataFrame:
    """Reads the add-to-carts data from a JSON source."""
    return spark.read.schema(add_to_carts_schema).json(path)

def read_previous_orders(spark: SparkSession, path: str) -> DataFrame:
    """Reads the previous orders data from a Parquet source."""
    return spark.read.schema(previous_orders_schema).parquet(path)