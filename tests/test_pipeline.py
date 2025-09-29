"""
This module contains tests for the Spark pipeline logic.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, date

from src.transformers import (
    unify_actions,
    get_customer_actions,
    get_impressions_for_date,
)
from src.pipeline import create_training_data
from src.schemas import (
    impressions_schema,
    clicks_schema,
    add_to_carts_schema,
    previous_orders_schema,
)

@pytest.fixture(scope="module")
def sample_config():
    """Provides a sample configuration for tests."""
    return {
        "pipeline": {
            "action_history_length": 1000,
            "salt_buckets": 2,
        }
    }

@pytest.fixture(scope="module")
def sample_data(spark_session: SparkSession):
    """
    Creates a comprehensive and reusable set of sample dataframes that cover
    key edge cases for the pipeline.
    """
    impressions_data = [
        ("dt=2025-08-16", "ranking1", 1, [{"item_id": 101, "is_order": False}, {"item_id": 102, "is_order": True}]),
        ("dt=2025-08-16", "ranking2", 2, [{"item_id": 103, "is_order": False}]),
        ("dt=2025-08-16", "ranking3", 3, [{"item_id": 104, "is_order": False}]),
        ("dt=2025-08-15", "ranking4", 4, [{"item_id": 105, "is_order": False}]),
        ("dt=2025-08-16", "ranking5", 5, [{"item_id": 106, "is_order": False}]),
    ]
    
    clicks_data = [
        ("dt=2025-08-15", 1, 101, datetime(2025, 8, 15, 12, 0, 0)),
        *[( "dt=2025-08-15", 1, 200 + i, datetime(2025, 8, 15, 14, 0, 0)) for i in range(1200)],
        ("dt=2025-08-14", 2, 103, datetime(2025, 8, 14, 10, 0, 0)),
        ("dt=2025-08-16", 5, 501, datetime(2025, 8, 16, 11, 0, 0)),
    ]
    
    add_to_carts_data = [
        ("dt=2025-08-15", 1, 102, 202, datetime(2025, 8, 15, 13, 0, 0)),
        ("dt=2025-08-13", 2, 104, 204, datetime(2025, 8, 13, 11, 0, 0)),
    ]
    
    previous_orders_data = [
        (date(2025, 8, 14), 2, 105),
    ]

    return {
        "impressions": spark_session.createDataFrame(impressions_data, impressions_schema),
        "clicks": spark_session.createDataFrame(clicks_data, clicks_schema),
        "add_to_carts": spark_session.createDataFrame(add_to_carts_data, add_to_carts_schema),
        "previous_orders": spark_session.createDataFrame(previous_orders_data, previous_orders_schema),
        "empty_clicks": spark_session.createDataFrame([], clicks_schema),
    }

class TestPipelineFunctions:
    """A class to group related pipeline tests."""

    def test_unify_actions(self, sample_data):
        """
        Tests the unify_actions function to ensure all action types are combined correctly.
        """
        actions_df = unify_actions(
            sample_data["clicks"], sample_data["add_to_carts"], sample_data["previous_orders"]
        )
        
        assert actions_df.count() == 1206
        assert actions_df.filter(col("action_type") == 1).count() == 1203
        assert actions_df.filter(col("action_type") == 2).count() == 2
        assert actions_df.filter(col("action_type") == 3).count() == 1

    def test_get_impressions_for_date(self, sample_data):
        """
        Tests that only impressions for the correct process_date are filtered and exploded.
        """
        impressions_on_date = get_impressions_for_date(sample_data["impressions"], "2025-08-16")
        
        assert impressions_on_date.count() == 5
        assert impressions_on_date.filter(col("customer_id") == 4).count() == 0

    def test_get_customer_actions(self, sample_data, sample_config):
        """
        Tests the core logic of creating padded action sequences and preventing data leaks.
        """
        actions_df = unify_actions(
            sample_data["clicks"], sample_data["add_to_carts"], sample_data["previous_orders"]
        )
        customer_actions = get_customer_actions(
            actions_df, "2025-08-16", **sample_config["pipeline"]
        )
        
        # Test customer 1 (more than 1000 actions)
        customer_1_row = customer_actions.filter(col("customer_id") == 1).first()
        assert customer_1_row is not None
        assert len(customer_1_row.actions) == sample_config["pipeline"]["action_history_length"]
        assert 0 not in customer_1_row.actions

        # Test customer 2 (3 actions)
        customer_2_row = customer_actions.filter(col("customer_id") == 2).first()
        assert customer_2_row is not None
        assert len(customer_2_row.actions) == sample_config["pipeline"]["action_history_length"]
        assert customer_2_row.actions[2] != 0 # First 3 actions are real
        assert customer_2_row.actions[3] == 0  # Check for padding

        # Test customer 5 (data leak prevention)
        customer_5_row = customer_actions.filter(col("customer_id") == 5).first()
        assert customer_5_row is None

    def test_create_training_data_integration(self, sample_data, sample_config):
        """
        An integration test to ensure the full pipeline handles all cases correctly.
        """
        training_df = create_training_data(
            sample_data["impressions"],
            sample_data["clicks"],
            sample_data["add_to_carts"],
            sample_data["previous_orders"],
            "2025-08-16",
            sample_config,
        )
        
        assert training_df.count() == 5
        
        # Test customer 3 (no actions) ensures nulls are correctly filled
        customer_3_row = training_df.filter(col("customer_id") == 3).first()
        assert customer_3_row is not None
        assert all(v == 0 for v in customer_3_row.actions)
        
        # Test customer 5 (no historical actions) is also correctly filled with zeros
        customer_5_row = training_df.filter(col("customer_id") == 5).first()
        assert customer_5_row is not None
        assert all(v == 0 for v in customer_5_row.actions)
