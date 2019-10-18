import itertools

import giraffe.configuration.common_testing_artifactrs as commons
import pytest
from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.data_access.redis_db import RedisDB
from redis import Redis

config = ConfigHelper()


@pytest.fixture(scope="session", autouse=True)
def init__and_finalize():
    yield  # Commands beyond this line will be called after the last test
    commons.log.debug('Closing redis driver.')
    commons.redis_driver.close()


@pytest.fixture(autouse=True)
def run_around_tests():
    commons.delete_redis_test_data()
    commons.init_redis_test_data()
    yield


def test_redis_db_connection():
    r: Redis = commons.redis_driver
    assert r.ping()


def test_order_jobs():
    correct_order = [
        'MyJob<nodes>',
        'MyJob<nodes>',
        'MyJob<nodes>',
        'MyJob<edges>',
        'MyJob<edges>',
    ]

    # After shuffling we expect the sort to bring the list to its original (correct) order

    for shuffled_list in list(itertools.permutations(correct_order, len(correct_order))):
        ordered_jobs = sorted(shuffled_list, key=IngestionManager.order_jobs, reverse=False)
        assert ordered_jobs == correct_order


def test_delete_keys():
    db: RedisDB = commons.redis_db
    r: Redis = commons.redis_driver
    commons.delete_redis_test_data()
    commons.init_redis_test_data()
    keys_to_delete = [key.decode(config.string_encoding) for key in r.keys(pattern=f'{config.test_job_name}*')]
    db.delete_keys(keys=keys_to_delete)
    after_deletion = (key.decode(config.string_encoding) for key in r.keys(pattern='test_key*'))
    assert any(after_deletion) is False


def test_pull_in_batches():
    db: RedisDB = commons.redis_db
    nodes_iterator = db.pull_in_batches(key_pattern=f'{config.test_job_name}:{config.nodes_ingestion_operation}:{config.test_labels[0]}', batch_size=500)
    nodes = [node.decode(config.string_encoding) for node in nodes_iterator]
    assert len(nodes) == len(commons.test_nodes)
