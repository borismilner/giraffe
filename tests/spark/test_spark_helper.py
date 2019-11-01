import time

import pytest
from giraffe.helpers import log_helper
from giraffe.helpers.spark_helper import SparkHelper
from redis import Redis
from giraffe.helpers.config_helper import ConfigHelper
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
import giraffe.configuration.common_testing_artifactrs as commons

config = ConfigHelper()
spark_helper = SparkHelper()


@pytest.fixture(scope="session", autouse=True)
def init_and_finalize():
    commons.log = log_helper.get_logger(logger_name='testing')
    yield  # Commands beyond this line will be called after the last test


@pytest.fixture(autouse=True)
def run_around_tests():
    commons.delete_elastic_test_data()
    commons.init_elastic_test_data()
    yield
    commons.init_elastic_test_data()


def test_get_spark_session():
    spark_session = spark_helper.get_spark_session()
    assert isinstance(spark_session, SparkSession)


def test_read_elasticsearch_index():
    commons.delete_elastic_test_data()
    commons.init_elastic_test_data()
    df: DataFrame = spark_helper.read_elasticsearch_index(index_name=config.test_elasticsearch_index)
    assert isinstance(df, DataFrame)
    pd_df = df.toPandas()
    rows, columns = pd_df.shape[0], pd_df.shape[1]
    assert rows == len(commons.test_nodes)  # Expected number of rows
    assert columns == len(commons.test_nodes[0].keys())  # Expected number of columns
    for column_name in pd_df.columns:
        assert column_name in commons.test_nodes[0].keys()
