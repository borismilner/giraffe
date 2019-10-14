from logging import Logger

import pytest
from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper

log: Logger
test_label = 'TEST_LABEL'
test_edge_type = 'TEST_EDGE'
test_property = 'age'
neo: neo_db.NeoDB
number_of_test_nodes = 1000

test_nodes = [{'_uid': i, '_label': test_label, 'age': i ** 2} for i in range(0, number_of_test_nodes)]
test_edges = [{'_fromUid': i, '_toUid': i * 2, '_edgeType': test_edge_type} for i in range(0, int(number_of_test_nodes / 2))]


def purge_test_data():
    global log, neo
    log.debug(f'Purging all nodes with label: {test_label}')
    query = f'MATCH (node:{test_label}) DETACH DELETE node'
    summary = neo.run_query(query=query)
    log.debug(f'Removed {summary.counters.nodes_deleted} {test_label} nodes.')


@pytest.fixture(scope="session", autouse=True)
def init__and_finalize():
    global log, neo
    log = log_helper.get_logger(logger_name='testing')
    neo = neo_db.NeoDB()
    purge_test_data()
    yield  # Commands beyond this line will be called after the last test
    purge_test_data()
    neo.close()


def test_neo_connection():
    global log
    # noinspection PyUnusedLocal
    is_service_available = False
    try:
        _ = neo_db.NeoDB()
        is_service_available = True
    except TechnicalError as e:
        log.error(str(e))
        pytest.fail(str(e))
    assert is_service_available is True


def test_merge_nodes():
    global log, neo
    db: neo_db.NeoDB = neo
    summary = db.merge_nodes(nodes=test_nodes, label=test_label)
    assert summary.counters.nodes_created == len(test_nodes)


def test_merge_edges():
    global log, neo
    db: neo_db.NeoDB = neo
    summary = db.merge_edges(edges=test_edges)
    pass


def test_create_index_if_not_exists():
    global log, neo
    db: neo_db.NeoDB = neo
    if db.is_index_exists(label=test_label, property_name=test_property):
        db.drop_index(label=test_label, property_name=test_property)
    summary = db.create_index_if_not_exists(label=test_label, property_name=test_property)
    assert summary.counters.indexes_added == 1
    summary = db.drop_index(label=test_label, property_name=test_property)
    assert summary.counters.indexes_removed == 1
