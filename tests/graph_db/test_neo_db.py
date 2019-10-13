import pytest
from giraffe.exceptions.technical_error import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper


def test_neo_connection():
    log = log_helper.get_logger(logger_name='testing')
    # noinspection PyUnusedLocal
    is_service_available = False
    try:
        _ = neo_db.Neo(host_address='127.0.0.1', username='neo4j', password='098098')
        is_service_available = True
    except TechnicalError as e:
        log.error(str(e))
        pytest.fail(str(e))
    assert is_service_available is True
