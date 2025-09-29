"""
This module is the entry point for the Spark pipeline.
"""
import argparse
import logging
import sys
import os
from pyspark.sql import SparkSession

from src.utils import def_spark_session, load_config
from src.readers import (
    read_impressions,
    read_clicks,
    read_add_to_carts,
    read_previous_orders,
)
from src.pipeline import create_training_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="PySpark Data Pipeline")
    parser.add_argument("--impressions-path", required=True)
    parser.add_argument("--clicks-path", required=True)
    parser.add_argument("--add-to-carts-path", required=True)
    parser.add_argument("--previous-orders-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--process-date", required=True)
    parser.add_argument("--config-path", default="config.yaml")
    return parser.parse_args()

def main(spark: SparkSession, args: argparse.Namespace):
    """Main function for the Spark pipeline."""
    logging.info("Starting data pipeline for process date: %s", args.process_date)
    config = load_config(args.config_path)

    impressions_df = read_impressions(spark, args.impressions_path)
    clicks_df = read_clicks(spark, args.clicks_path)
    add_to_carts_df = read_add_to_carts(spark, args.add_to_carts_path)
    previous_orders_df = read_previous_orders(spark, args.previous_orders_path)

    training_df = create_training_data(
        impressions_df,
        clicks_df,
        add_to_carts_df,
        previous_orders_df,
        args.process_date,
        config,
    )

    output_path = os.path.join(args.output_path, f"dt={args.process_date}")
    logging.info("Writing output data to: %s", output_path)
    training_df.write.mode("overwrite").parquet(output_path)
    logging.info("Output data written successfully.")

if __name__ == "__main__":
    cli_args = parse_args()
    spark = None
    try:
        config = load_config(cli_args.config_path)
        spark = def_spark_session(config["spark"]["app_name"])
        main(spark, cli_args)
    except Exception as e:
        logging.error("Pipeline execution failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
