import giraffe.configuration.common_testing_artifactrs as commons
import pandas as pd
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.types import DecimalType


@pytest.fixture(autouse=True)
def run_around_tests(logger, elasticsearch):
    commons.purge_elasticsearch_database(es=elasticsearch, log=logger)
    commons.init_elastic_test_data(es=elasticsearch)
    yield
    commons.init_elastic_test_data(es=elasticsearch)


def test_get_spark_session(spark_helper):
    spark_session = spark_helper.get_spark_session()
    assert isinstance(spark_session, SparkSession)


def test_read_elasticsearch_index(config_helper, spark_helper, nodes):
    df: DataFrame = spark_helper.read_df_from_elasticsearch_index(index_name=config_helper.test_elasticsearch_index)
    assert isinstance(df, DataFrame)
    pd_df = df.toPandas()
    expected_num_rows, expected_num_columns = pd_df.shape[0], pd_df.shape[1]
    assert expected_num_rows == len(nodes)
    assert expected_num_columns == len(nodes[0].keys())
    for column_name in pd_df.columns:
        assert column_name in nodes[0].keys()


def test_read_from_es_write_to_redis(config_helper, redis_driver, spark_helper, redis_db, logger):
    commons.delete_redis_keys_prefix(prefix=f'{config_helper.test_redis_table_prefix}*', redis_db=redis_db, log=logger)
    df: DataFrame = spark_helper.read_df_from_elasticsearch_index(index_name=config_helper.test_elasticsearch_index)
    spark_helper.write_df_to_redis(df=df, key_prefix=config_helper.test_redis_table_prefix)
    num_keys_written = 0
    for _ in redis_driver.scan_iter(f'{config_helper.test_redis_table_prefix}*'):  # Notice how you can't access the whole group in O(1) as opposed to a SCARD!
        num_keys_written += 1
    assert num_keys_written == config_helper.number_of_test_nodes


def test_add_column_as_python_dict_string(spark_helper):
    how_many = 100
    dict_str_column = 'as_string'

    data = [[f'Person-{i}', f'{i}', [f'Fruit-{i}', f'Game-{i}'], f'19/12/{1970 + i}'] for i in range(0, how_many)]
    pandas_df = pd.DataFrame(data, columns=['String', 'Integer', 'List', 'Timestamp'])
    pandas_df['String'] = pandas_df['String'].astype('str')
    pandas_df['Timestamp'] = pd.to_datetime(pandas_df['Timestamp'])
    pandas_df['Integer'] = pd.to_numeric(pandas_df['Integer'])

    spark_df = spark_helper.get_spark_session().createDataFrame(data=pandas_df)
    spark_df = spark_df.withColumn("Decimal", spark_df["Integer"].cast(DecimalType()))
    spark_df = spark_df.withColumn("DateType", spark_df["Timestamp"].cast(DateType()))

    df = spark_helper.get_string_dict_dataframe(df=spark_df, column_name=dict_str_column)
    assert df.count() == how_many
    assert dict_str_column in df.columns
    # TODO: add informative assertions!
