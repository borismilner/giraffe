import time
from logging import Logger

import pytest
from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper

log: Logger
test_label = 'TEST_LABEL'
test_edge_type = 'TEST_EDGE'
neo: neo_db.NeoDB

test_nodes = [{'_uid': i, '_label': test_label} for i in range(0, 100)]


@pytest.fixture(scope="session", autouse=True)
def do_something():
    global log, neo
    log = log_helper.get_logger(logger_name='testing')
    neo = neo_db.NeoDB()
    log.debug(f'Purging all nodes with label: {test_label}')
    query = f'MATCH (node:{test_label}) DETACH DELETE node'
    summary = neo.run_query(query=query)
    log.debug(f'Removed {summary.counters.nodes_deleted} {test_label} nodes.')


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
    nodes = [
        {'_uid': 1, '_label': test_label, 'name': 'Boris', 'has': 'tv', 'birthday': time.time()},
        {'_uid': 2, '_label': test_label, 'name': 'Milner', 'has': 'laptop'},
    ]
    summary = neo.merge_nodes(nodes=nodes)
    pass


def test_merge_edges():
    global log, neo
