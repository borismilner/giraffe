import itertools
import threading

import giraffe.configuration.common_testing_artifactrs as commons
import pytest
from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.data_access.redis_db import RedisDB
from redis import Redis


@pytest.fixture(autouse=True)
def run_around_tests():
    commons.purge_redis_database()
    commons.init_redis_test_data()
    yield


def test_redis_db_connection(redis_driver):
    r: Redis = redis_driver
    assert r.ping()


def test_order_jobs():
    correct_order = [
            'test_table:nodes_ingest:Person1',
            'test_table:nodes_ingest:Person2',
            'test_table:nodes_ingest:Person3',
            'test_table:nodes_ingest:Person4',
            'test_table:edges_ingest:Person1',
            'test_table:edges_ingest:Person2',
            'test_table:edges_ingest:Person3',
    ]

    # After shuffling, we expect the sort to bring the list to its original (correct) order

    for shuffled_list in list(itertools.permutations(correct_order, len(correct_order))):
        ordered_jobs = sorted(shuffled_list, key=lambda item: (IngestionManager.order_jobs(item), str.lower(item)), reverse=False)
        assert ordered_jobs == correct_order


def test_delete_keys(redis_db, redis_driver, config_helper):
    db: RedisDB = redis_db
    r: Redis = redis_driver
    commons.purge_redis_database()
    commons.init_redis_test_data()
    keys_to_delete = [key for key in r.keys(pattern=f'{config_helper.test_job_name}*')]
    db.delete_keys(keys=keys_to_delete)
    after_deletion = (key for key in r.keys(pattern='test_key*'))
    assert any(after_deletion) is False


def test_pull_in_batches(redis_db, config_helper, nodes):
    db: RedisDB = redis_db
    nodes_iterator = db.pull_set_members_in_batches(key_pattern=f'{config_helper.test_job_name}:{config_helper.nodes_ingestion_operation}:{config_helper.test_labels[0]}', batch_size=500)
    nodes = [node for node in nodes_iterator]
    assert len(nodes) == len(nodes)


def test_pull_batch_values_by_keys(redis_db):
    db: RedisDB = redis_db
    how_many_keys = 1000
    for i in range(0, how_many_keys):
        db.driver.set(name=f'Person-{i}', value=f'This is person-{i}')

    values = db.pull_batch_values_by_keys(keys=[f'Person-{i}' for i in range(0, how_many_keys)])
    assert len(values) == how_many_keys


# NOTE: Works only with redis 5.0+
def test_pull_batch_from_stream(redis_db, redis_driver, config_helper, nodes):
    r: Redis = redis_driver
    db: RedisDB = redis_db
    r.flushall()

    received_entities = []

    batch_size = 100

    def collect_events():
        nonlocal batch_size
        nonlocal received_entities
        for batch in db.pull_batch_from_stream(stream_name=config_helper.test_redis_stream_name,
                                               batch_size=batch_size,
                                               block_milliseconds=3000):
            received_entities.extend(batch)

    t = threading.Thread(target=collect_events)
    t.start()  # Collecting the events in real-time
    for node in nodes:
        r.xadd(name=config_helper.test_redis_stream_name, fields=node)
    t.join()

    assert len(received_entities) == config_helper.number_of_test_nodes
