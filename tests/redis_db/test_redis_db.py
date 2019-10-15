import math
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
    request_id = config.test_request_id
    bathes = list_as_chunks(the_list=test_nodes, chunk_size=config.test_chunk_size)
    for i, batch in enumerate(bathes):
        db.populate_sorted_set(key=f'{request_id}:Batch_{i}', score=0, values=batch)


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


def test_populate_sorted_set():
    global log, redis_db, redis_driver
    delete_test_data()
    db: RedisDB = redis_db
    r: Redis = redis_driver
    request_id = config.test_request_id
    bathes = list_as_chunks(the_list=test_nodes, chunk_size=config.test_chunk_size)

    expected_keys = []

    for i, batch in enumerate(bathes):
        key = f'{request_id}:Batch_{i}'
        expected_keys.append(key)
        db.populate_sorted_set(key=key, score=0, values=batch)

    db_keys = r.keys()
    assert len(db_keys) == math.ceil(config.number_of_test_nodes / config.test_chunk_size)
    assert set(expected_keys) == set(key.decode('utf8') for key in db_keys)
