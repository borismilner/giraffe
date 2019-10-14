from logging import Logger

import pytest
from giraffe.exceptions.technical_error import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper

log: Logger


@pytest.fixture(scope="session", autouse=True)
def do_something(request):
    global log
    log = log_helper.get_logger(logger_name='testing')


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
    global log
    neo = neo_db.NeoDB()
    nodes = [{'_uid': 1, 'name': 'Boris', 'boom': 'tv'}]
    neo.merge_nodes(nodes=nodes)
