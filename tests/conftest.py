"""
This module contains fixtures for the tests.
"""

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session(request):
    """
    Creates a SparkSession for the tests and ensures it's stopped at the end.
    The 'session' scope means this fixture is created once for the entire test run,
    which is highly efficient.
    """
    spark = SparkSession.builder.appName("CodingChallengeTests").getOrCreate()
    
    # request.addfinalizer ensures that the stop() function is called when the
    # test session finishes, regardless of whether the tests passed or failed.
    request.addfinalizer(lambda: spark.stop())
    
    return spark