import math
import itertools
from logging import Logger

import pytest
from giraffe.configuration.common_testing_artifactrs import *
from giraffe.helpers.utilities import list_as_chunks
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

    for request_id, graph_entities in ((config.test_request_id_for_nodes, test_nodes),
                                       (config.test_request_id_for_edges, test_edges)):
        bathes = list_as_chunks(the_list=graph_entities, chunk_size=config.test_chunk_size)
        for i, batch in enumerate(bathes):
            db.populate_ordered_set(key=f'{request_id}:Batch[{i}]', score=0, values=batch)


@pytest.fixture(autouse=True)
def run_around_tests():
    global log, redis_db, redis_driver
    delete_test_data()
    yield
    init_test_data()


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


def test_populate_ordered_set():
    global log, redis_db, redis_driver
    delete_test_data()
    db: RedisDB = redis_db
    r: Redis = redis_driver
    request_id = config.test_request_id_for_nodes

    expected_keys = []

    how_many_streams = 100
    for i in range(0, how_many_streams):
        key = f'{request_id}[{i}]'
        expected_keys.append(key)
        db.populate_ordered_set(key=key, score=0, values=test_nodes)

    db_keys = r.keys()
    assert len(db_keys) == how_many_streams
    assert set(expected_keys) == set(key.decode('utf8') for key in db_keys)
    for key in db_keys:
        size = r.zcard(key)
        assert size == len(test_nodes)
