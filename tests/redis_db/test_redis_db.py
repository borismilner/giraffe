import itertools
from logging import Logger

import pytest
from giraffe.configuration.common_testing_artifactrs import *
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.tools.redis_db import RedisDB
from redis import Redis

config = ConfigHelper()

log: Logger
redis_db: RedisDB
redis_driver: Redis


@pytest.fixture(scope="session", autouse=True)
def init__and_finalize():
    global log, redis_db, redis_driver
    log = log_helper.get_logger(logger_name='testing_redis')
    redis_db = RedisDB()
    redis_driver = redis_db.get_driver()
    yield  # Commands beyond this line will be called after the last test
    log.debug('Closing redis driver.')
    redis_driver.close()


def delete_test_data():
    global log, redis_db, redis_driver
    db: RedisDB = redis_db
    db.purge_all()


def init_test_data():
    global log, redis_db, redis_driver
    db: RedisDB = redis_db

    # Populate nodes
    db.populate_job(job_name=config.test_job_name,
                    operation_required='nodes_ingest',
                    operation_arguments='officer, gentleman',
                    items=[str(value) for value in test_nodes])

    # Populate edges
    db.populate_job(job_name=config.test_job_name,
                    operation_required='edges_ingest',
                    operation_arguments=f'{config.test_edge_type},{config.test_labels[0]},{config.test_labels[0]}',
                    items=[str(value) for value in test_edges])


@pytest.fixture(autouse=True)
def run_around_tests():
    global log, redis_db, redis_driver
    delete_test_data()
    init_test_data()
    yield


def test_redis_db_connection():
    global log, redis_db, redis_driver
    r: Redis = redis_driver
    assert r.ping()


def test_order_jobs():
    global log, redis_db, redis_driver
    db: RedisDB = redis_db
    correct_order = [
        'MyJob<nodes>:Batch[1]',
        'MyJob<nodes>:Batch[2]',
        'MyJob<nodes>:Batch[3]',
        'MyJob<edges>:Batch[1]',
        'MyJob<edges>:Batch[2]',
    ]

    # After shuffling we expect the sort to bring the list to its original (correct) order

    for shuffled_list in list(itertools.permutations(correct_order, len(correct_order))):
        ordered_jobs = sorted(shuffled_list, key=db.order_jobs, reverse=False)
        assert ordered_jobs == correct_order


def test_delete_keys():
    global log, redis_db, redis_driver
    db: RedisDB = redis_db
    r: Redis = redis_driver
    delete_test_data()
    init_test_data()
    keys_to_delete = [key.decode('utf8') for key in r.keys(pattern=f'{config.test_job_name}*')]
    db.delete_keys(keys=keys_to_delete)
    after_deletion = (key.decode('utf8') for key in r.keys(pattern='test_key*'))
    assert any(after_deletion) is False


def test_populate_job():
    global log, redis_db, redis_driver
    db: RedisDB = redis_db
    r: Redis = redis_driver
    delete_test_data()

    # Populate nodes
    db.populate_job(job_name=config.test_job_name,
                    operation_required='nodes_ingest',
                    operation_arguments='officer, gentleman',
                    items=[str(value) for value in test_nodes])

    # Populate edges
    db.populate_job(job_name=config.test_job_name,
                    operation_required='edges_ingest',
                    operation_arguments=f'{config.test_edge_type},{config.test_labels[0]},{config.test_labels[1]}',
                    items=[str(value) for value in test_edges])

    keys = r.keys(pattern=f'{config.test_job_name}*')
    assert len(keys) == 2
    node_keys = r.keys(pattern=f'{config.test_job_name}:{config.nodes_ingestion_operation}:*')
    assert len(node_keys) == 1
    edges_keys = r.keys(pattern=f'{config.test_job_name}:{config.edges_ingestion_operation}:*')
    assert len(edges_keys) == 1

    nodes_key = node_keys[0].decode('utf8')
    edges_key = edges_keys[0].decode('utf8')

    num_stored_nodes = r.scard(name=nodes_key)
    assert num_stored_nodes == len(test_nodes)
    num_stored_edges = r.scard(name=edges_key)
    assert num_stored_edges == len(test_edges)


def test_pull_in_batches():
    global log, redis_db, redis_driver
    db: RedisDB = redis_db
    nodes_iterator = db.pull_in_batches(key='Awesome:nodes_ingest:officer, gentleman', batch_size=500)
    nodes = [node.decode('utf8') for node in nodes_iterator]
    assert len(nodes) == len(test_nodes)
