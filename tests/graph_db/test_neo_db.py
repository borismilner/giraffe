from logging import Logger

import pytest
from giraffe.configuration.common_testing_artifactrs import *
from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper

config = ConfigHelper()

log: Logger
neo: neo_db.NeoDB


def delete_test_data():
    global log, neo
    label_to_delete = config.test_labels[0]
    log.debug(f'Purging all nodes with label: {label_to_delete}')
    query = f'MATCH (node:{label_to_delete}) DETACH DELETE node'
    summary = neo.run_query(query=query)
    log.debug(f'Removed {summary.counters.nodes_deleted} {label_to_delete} nodes.')


def init_test_data():
    global log, neo
    db: neo_db.NeoDB = neo
    db.merge_nodes(nodes=test_nodes, label=config.test_labels[0])


@pytest.fixture(scope="session", autouse=True)
def init__and_finalize():
    global log, neo
    log = log_helper.get_logger(logger_name='testing')
    neo = neo_db.NeoDB()
    delete_test_data()
    yield  # Commands beyond this line will be called after the last test
    delete_test_data()


@pytest.fixture(autouse=True)
def run_around_tests():
    init_test_data()
    yield
    delete_test_data()


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
    delete_test_data()
    summary = db.merge_nodes(nodes=test_nodes, label=config.test_labels[0])
    assert summary.counters.nodes_created == len(test_nodes)


def test_merge_edges():
    global log, neo
    db: neo_db.NeoDB = neo
    summary = db.merge_edges(edges=test_edges, from_label=config.test_labels[0], to_label=config.test_labels[0])
    assert summary.counters.relationships_created == config.number_of_test_edges


def test_create_index_if_not_exists():
    global log, neo
    db: neo_db.NeoDB = neo
    db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    summary = db.create_index_if_not_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary.counters.indexes_added == 1
    summary = db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary.counters.indexes_removed == 1


def test_drop_index_if_exists():
    global log, neo
    db: neo_db.NeoDB = neo
    db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    summary = db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary is None
    db.create_index_if_not_exists(label=config.test_labels[0], property_name=config.test_property)
    summary = db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary.counters.indexes_removed == 1
