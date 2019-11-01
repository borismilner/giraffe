import pytest
from giraffe.helpers import log_helper
from giraffe.helpers.spark_helper import SparkHelper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.data_access.redis_db import RedisDB
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
import giraffe.configuration.common_testing_artifactrs as commons

config = ConfigHelper()
spark_helper = SparkHelper()
r = RedisDB().get_driver()


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
    df: DataFrame = spark_helper.read_df_from_elasticsearch_index(index_name=config.test_elasticsearch_index)
    assert isinstance(df, DataFrame)
    pd_df = df.toPandas()
    expected_num_rows, expected_num_columns = pd_df.shape[0], pd_df.shape[1]
    assert expected_num_rows == len(commons.test_nodes)
    assert expected_num_columns == len(commons.test_nodes[0].keys())
    for column_name in pd_df.columns:
        assert column_name in commons.test_nodes[0].keys()


def test_read_from_es_write_to_redis():
    commons.delete_redis_keys_prefix(prefix=f'{config.test_redis_table_prefix}*')
    df: DataFrame = spark_helper.read_df_from_elasticsearch_index(index_name=config.test_elasticsearch_index)
    spark_helper.write_df_to_redis(df=df, key_prefix=config.test_redis_table_prefix)
    num_keys_written = 0
    for _ in r.scan_iter(f'{config.test_redis_table_prefix}*'):  # Notice how you can't access the whole group in O(1) as opposed to a SCARD!
        num_keys_written += 1
    assert num_keys_written == config.number_of_test_nodes
