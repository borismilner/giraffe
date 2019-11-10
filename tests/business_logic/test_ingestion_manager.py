import time
import pytest
from multiprocessing import Process
from giraffe.helpers import log_helper
from giraffe.helpers.spark_helper import SparkHelper
from pyspark.sql import DataFrame
from redis import Redis
from giraffe.helpers.config_helper import ConfigHelper
import giraffe.configuration.common_testing_artifactrs as commons
from giraffe.business_logic.ingestion_manger import IngestionManager

config = ConfigHelper()


@pytest.fixture(scope="session", autouse=True)
def init_and_finalize():
    commons.log = log_helper.get_logger(logger_name='testing')
    # commons.neo = neo_db.NeoDB()
    # commons.delete_neo_test_data()
    yield  # Commands beyond this line will be called after the last test
    # commons.delete_neo_test_data()


@pytest.fixture(autouse=True)
def run_around_tests():
    # commons.init_test_data()
    yield
    # commons.delete_neo_test_data()


def test_parse_redis_key():
    im = IngestionManager()
    job_name = config.nodes_ingestion_operation
    operation = config.nodes_ingestion_operation
    labels = config.test_labels
    parsed: IngestionManager.key_elements_type = im.parse_redis_key(
        key=f'{job_name}{config.key_separator}{operation}{config.key_separator}{",".join(labels)}')
    assert parsed.job_name == job_name
    assert parsed.operation == operation
    assert set(parsed.arguments) == set(labels)


def test_publish_job():
    r: Redis = commons.redis_driver
    im: IngestionManager = commons.ingestion_manager

    commons.delete_redis_test_data()

    # Populate nodes
    im.publish_job(job_name=config.test_job_name,
                   operation=config.nodes_ingestion_operation,
                   operation_arguments=','.join(config.test_labels),
                   items=[str(value) for value in commons.test_nodes])

    # Populate edges
    im.publish_job(job_name=config.test_job_name,
                   operation=config.edges_ingestion_operation,
                   operation_arguments=f'{config.test_edge_type},{config.test_labels[0]}',
                   items=[str(value) for value in commons.test_edges])

    keys = r.keys(pattern=f'{config.test_job_name}*')
    assert len(keys) == 2
    node_keys = r.keys(pattern=f'{config.test_job_name}{config.key_separator}{config.nodes_ingestion_operation}{config.key_separator}*')
    assert len(node_keys) == 1
    edges_keys = r.keys(pattern=f'{config.test_job_name}{config.key_separator}{config.edges_ingestion_operation}{config.key_separator}*')
    assert len(edges_keys) == 1

    nodes_key = node_keys[0]
    edges_key = edges_keys[0]

    num_stored_nodes = r.scard(name=nodes_key)
    assert num_stored_nodes == len(commons.test_nodes)
    num_stored_edges = r.scard(name=edges_key)
    assert num_stored_edges == len(commons.test_edges)


def test_process_job():
    commons.delete_redis_test_data()
    commons.delete_neo_test_data()
    commons.init_redis_test_data()
    im = commons.IngestionManager()
    im.process_job(job_name=config.test_job_name)
    query = f'MATCH (:{commons.config.test_labels[0]}) RETURN COUNT(*) AS count'
    count = commons.neo.pull_query(query=query).value()[0]
    assert count == config.number_of_test_nodes
    query = f'MATCH ()-[:{commons.config.test_edge_type}]-() RETURN COUNT(*) AS count'
    count = commons.neo.pull_query(query=query).value()[0]
    assert count == config.number_of_test_edges


def test_process_spark_redis_table():
    job_name = config.test_redis_table_prefix.split(config.key_separator)[0]
    # Initialization
    spark_helper = SparkHelper()
    # Delete both redis and neo
    commons.init_elastic_test_data()
    commons.delete_redis_test_data()
    commons.delete_neo_test_data()
    # Ingestion manager
    im = commons.IngestionManager()

    # Populate a spark data-frame from an index in Elasticsearch
    df: DataFrame = spark_helper.read_df_from_elasticsearch_index(index_name=config.test_elasticsearch_index)

    # Write the data-frame into redis in a "table" format - the _uid being last part of the key
    spark_helper.write_df_to_redis(df=df, key_prefix=config.test_redis_table_prefix)

    # Read the written-above nodes from redis and write them into Neo4j
    im.process_spark_redis_table(job_name=job_name, batch_size=50)
    query = f'MATCH (:{commons.config.test_labels[0]}) RETURN COUNT(*) AS count'
    count = commons.neo.pull_query(query=query).value()[0]
    assert count == config.number_of_test_nodes


def test_parallel_ingestion_managers():
    # commons.delete_neo_test_data()
    # im = commons.IngestionManager()

    num_records = 100_000
    batch_size = 50_000
    spark_helper = SparkHelper()
    sc = spark_helper.get_spark_session().sparkContext

    # .option("table", config.test_redis_table_prefix) \
    columns = ['_uid', 'name', 'age', 'email']
    for batch in range(1, num_records, batch_size):
        data = [(i, f'Person-{i}', i, f'Person-{i}@gmail.com') for i in range(batch, batch + batch_size)]
        df = sc.parallelize(data).toDF(columns)
        df.write.format("org.apache.spark.sql.redis") \
            .option("table", "second:nodes_ingest") \
            .option("key.column", '_uid') \
            .mode('Append') \
            .save()


def prepare_redis():
    num_records = 100_000
    batch_size = 50_000
    spark_helper = SparkHelper()
    sc = spark_helper.get_spark_session().sparkContext

    for key_name in ('first', 'second'):
        columns = ['_uid', 'name', 'age', 'email']
        for batch in range(1, num_records, batch_size):
            data = [(i, f'Person-{i}', i, f'Person-{i}@gmail.com') for i in range(batch, batch + batch_size)]
            df = sc.parallelize(data).toDF(columns)
            df.write.format("org.apache.spark.sql.redis") \
                .option("table", f"{key_name}:nodes_ingest:Person") \
                .option("key.column", '_uid') \
                .mode('Append') \
                .save()


def test_single_vs_multiple_redis_consumers():
    log = commons.log
    log.info('Deleting redis content.')
    commons.delete_redis_test_data()
    log.info('Deleting neo content.')
    commons.delete_neo_test_data()
    log.info('Populating redis.')
    prepare_redis()
    log.info('Done populating.')

    im = IngestionManager()
    t = time.time()
    im.process_spark_redis_table(job_name='first', batch_size=10_000)
    im.process_spark_redis_table(job_name='second', batch_size=10_000)
    one_by_one = time.time() - t
    log.info(f'One by one: {one_by_one}.')

    first = Process(target=im.process_spark_redis_table, args=('first', 10_000))
    second = Process(target=im.process_spark_redis_table, args=('second', 10_000))

    t = time.time()
    first.start()
    second.start()
    first.join()
    second.join()
    two_in_parallel = time.time() - t
    log.info(f'Two in parallel: {two_in_parallel}.')
