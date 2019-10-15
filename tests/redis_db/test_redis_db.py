from logging import Logger

import pytest
from giraffe.helpers import log_helper
from giraffe.tools.redis_db import RedisDB
from redis import Redis

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


@pytest.fixture(autouse=True)
def run_around_tests():
    # init_test_data()
    yield
    # delete_test_data()


def test_redis_db_connection():
    global log, redis_db, redis_driver
    r: Redis = redis_driver
    assert r.ping()
