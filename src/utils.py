"""
This module contains utility functions for the Spark pipeline.
"""

from pyspark.sql import SparkSession
import yaml

def def_spark_session(app_name: str = "PySparkApp") -> SparkSession:
    """
    Creates and returns a SparkSession object with a specified application name.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_config(path="config.yaml"):
    """Loads the configuration from a YAML file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)
