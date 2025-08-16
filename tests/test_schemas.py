"""
This module contains tests for the data schemas.
"""
from pyspark.sql.types import IntegerType, ArrayType, StructType

from src.schemas import (
    impressions_schema,
    clicks_schema,
    add_to_carts_schema,
    previous_orders_schema,
)

def test_impressions_schema_structure():
    """
    Validates the structure and key types of the impressions_schema.
    """
    assert impressions_schema is not None
    # Check that key fields have the expected data types.
    assert isinstance(impressions_schema["customer_id"].dataType, IntegerType)
    assert isinstance(impressions_schema["impressions"].dataType, ArrayType)
    assert isinstance(impressions_schema["impressions"].dataType.elementType, StructType)

def test_clicks_schema_structure():
    """
    Validates the structure and key types of the clicks_schema.
    """
    assert clicks_schema is not None
    assert isinstance(clicks_schema["customer_id"].dataType, IntegerType)
    assert isinstance(clicks_schema["item_id"].dataType, IntegerType)

def test_add_to_carts_schema_structure():
    """
    Validates the structure and key types of the add_to_carts_schema.
    """
    assert add_to_carts_schema is not None
    assert isinstance(add_to_carts_schema["customer_id"].dataType, IntegerType)
    assert isinstance(add_to_carts_schema["config_id"].dataType, IntegerType)

def test_previous_orders_schema_structure():
    """
    Validates the structure and key types of the previous_orders_schema.
    """
    assert previous_orders_schema is not None
    assert isinstance(previous_orders_schema["customer_id"].dataType, IntegerType)
    assert isinstance(previous_orders_schema["config_id"].dataType, IntegerType)