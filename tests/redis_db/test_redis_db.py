import itertools

import giraffe.configuration.common_testing_artifactrs as commons
import pytest
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.tools.redis_db import RedisDB
from redis import Redis

config = ConfigHelper()


@pytest.fixture(scope="session", autouse=True)
def init__and_finalize():
    yield  # Commands beyond this line will be called after the last test
    commons.log.debug('Closing redis driver.')
    commons.redis_driver.close()


@pytest.fixture(autouse=True)
def run_around_tests():
    commons.delete_test_data()
    commons.init_test_data()
    yield


def test_redis_db_connection():
    r: Redis = commons.redis_driver
    assert r.ping()


def test_order_jobs():
    db: RedisDB = commons.redis_db
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
    db: RedisDB = commons.redis_db
    r: Redis = commons.redis_driver
    commons.delete_test_data()
    commons.init_test_data()
    keys_to_delete = [key.decode('utf8') for key in r.keys(pattern=f'{config.test_job_name}*')]
    db.delete_keys(keys=keys_to_delete)
    after_deletion = (key.decode('utf8') for key in r.keys(pattern='test_key*'))
    assert any(after_deletion) is False


def test_pull_in_batches():
    db: RedisDB = commons.redis_db
    nodes_iterator = db.pull_in_batches(key=f'{config.test_job_name}:{config.nodes_ingestion_operation}:{config.test_labels[0]},{config.test_labels[1]}', batch_size=500)
    nodes = [node.decode('utf8') for node in nodes_iterator]
    assert len(nodes) == len(commons.test_nodes)
