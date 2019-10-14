import time
from logging import Logger

import pytest
from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper

log: Logger
test_label = 'DELETEME'
neo: neo_db.NeoDB


@pytest.fixture(scope="session", autouse=True)
def do_something():
    global log, neo
    log = log_helper.get_logger(logger_name='testing')
    neo = neo_db.NeoDB()
    log.debug(f'Deleting all nodes with label: {test_label}')
    query = f'MATCH (node:{test_label}) DETACH DELETE node'
    summary = neo.run_query(query=query)
    log.debug(f'Removed {summary.counters.nodes_deleted} nodes.')


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
        {'_uid': 1, 'name': 'Boris', 'has': 'tv', 'birthday': time.time()},
        {'_uid': 2, 'name': 'Milner', 'has': 'laptop'},
    ]
    neo.merge_nodes(nodes=nodes)
