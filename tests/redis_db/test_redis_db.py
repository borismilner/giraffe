import random
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

    db.populate_hashes(members=[
        (f'{config.test_request_name}{i}.[add_node].[{",".join(config.test_labels)}]', node)
        for i, node in enumerate(test_nodes)
    ])

    # Populate edges

    db.populate_hashes(members=[
        (f'{config.test_request_name}{i}.[add_edge].[{config.test_edge_type},{config.test_labels[0]},{config.test_labels[1]}]', node)
        for i, node in enumerate(test_edges)
    ])


@pytest.fixture(autouse=True)
def run_around_tests():
    global log, redis_db, redis_driver
    init_test_data()
    yield
    delete_test_data()


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


def test_populate_hashes():
    global log, redis_db, redis_driver
    db: RedisDB = redis_db
    r: Redis = redis_driver
    delete_test_data()

    init_test_data()

    keys = r.keys()

    assert len(keys) == len(test_nodes) + len(test_edges)
    assert len(set(keys)) == len(keys)

    for _ in range(0, random.randint(0, 100)):
        random_member_index = random.randint(0, min(len(test_nodes), len(test_edges)) - 1)
        hash_values = set(key.decode('utf8') for key in r.hgetall(keys[random_member_index]))
        original_node_keys = set(test_nodes[random_member_index].keys())
        original_edge_keys = set(test_edges[random_member_index].keys())
        assert hash_values == original_node_keys or hash_values == original_edge_keys
