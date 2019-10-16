import pytest
import giraffe.configuration.common_testing_artifactrs as commons
from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper

config = ConfigHelper()


def init_test_data():
    db: neo_db.NeoDB = commons.neo
    db.merge_nodes(nodes=commons.test_nodes, label=config.test_labels[0])


@pytest.fixture(scope="session", autouse=True)
def init__and_finalize():
    commons.log = log_helper.get_logger(logger_name='testing')
    commons.neo = neo_db.NeoDB()
    commons.delete_neo_test_data()
    yield  # Commands beyond this line will be called after the last test
    commons.delete_neo_test_data()


@pytest.fixture(autouse=True)
def run_around_tests():
    init_test_data()
    yield
    commons.delete_neo_test_data()


def test_neo_connection():
    log = commons.log
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
    neo = commons.neo
    db: neo_db.NeoDB = neo
    commons.delete_neo_test_data()
    summary = db.merge_nodes(nodes=commons.test_nodes, label=config.test_labels[0])
    assert summary.counters.nodes_created == len(commons.test_nodes)


def test_merge_edges():
    neo = commons.neo
    db: neo_db.NeoDB = neo
    summary = db.merge_edges(edges=commons.test_edges, from_label=config.test_labels[0], to_label=config.test_labels[0])
    assert summary.counters.relationships_created == config.number_of_test_edges


def test_create_index_if_not_exists():
    neo = commons.neo
    db: neo_db.NeoDB = neo
    db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    summary = db.create_index_if_not_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary.counters.indexes_added == 1
    summary = db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary.counters.indexes_removed == 1


def test_drop_index_if_exists():
    db: neo_db.NeoDB = commons.neo
    db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    summary = db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary is None
    db.create_index_if_not_exists(label=config.test_labels[0], property_name=config.test_property)
    summary = db.drop_index_if_exists(label=config.test_labels[0], property_name=config.test_property)
    assert summary.counters.indexes_removed == 1
