import pytest
from giraffe.exceptions.technical_error import TechnicalError
from giraffe.graph_db import neo_db
from giraffe.helpers import log_helper


def test_neo_connection():
    log = log_helper.get_logger(logger_name='testing')
    # noinspection PyUnusedLocal
    is_service_available = False
    try:
        _ = neo_db.NeoDB()
        is_service_available = True
    except TechnicalError as e:
        log.error(str(e))
        pytest.fail(str(e))
    assert is_service_available is True
