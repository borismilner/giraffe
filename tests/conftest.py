import os
import runpy
from logging import Logger
from multiprocessing import Process

from giraffe.helpers.EventDispatcher import EventDispatcher

import tests.business_logic as bl
import giraffe.configuration.common_testing_artifactrs as commons
import pytest
import requests
from giraffe.business_logic.data_to_entities_translators.mock_translator import MockDataToGraphEntitiesProvider
from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.data_access import neo_db
from giraffe.data_access.data_model_providers.mock_data_model_provider import MockDataAndModelProvider
from giraffe.data_access.redis_db import RedisDB
from giraffe.helpers import config_helper as config
from giraffe.helpers import log_helper
from giraffe.helpers.dev_spark_helper import DevSparkHelper
from giraffe.helpers.multi_helper import MultiHelper
from giraffe.monitoring.progress_monitor import ProgressMonitor

import tests.utilities.timing_utilities as timing_utils


@pytest.fixture(scope="session")
def config_helper():
    return config.get_config()


@pytest.fixture(scope="session")
def event_dispatcher():
    return EventDispatcher()


@pytest.fixture(scope="session")
def white_list_file_path():
    white_list_file_path = os.path.join(os.path.dirname(os.path.abspath(bl.__file__)), "white_list.txt")
    return white_list_file_path


@pytest.fixture(scope="session")
def progress_monitor(config_helper, event_dispatcher):
    progress_monitor = ProgressMonitor(event_dispatcher=event_dispatcher, config=config_helper)
    progress_monitor.task_started(request_id='unit-testing',
                                  request_type='white_list',
                                  request_content='nothing')
    return progress_monitor


@pytest.fixture(scope="session")
def multi_helper(config_helper):
    return MultiHelper(config=config_helper)


@pytest.fixture(scope="session")
def ingestion_endpoint(config_helper):
    return f'{config_helper.test_front_desk_address}:{config_helper.front_desk_port}/ingest'


@pytest.fixture(scope="session")
def neo(progress_monitor):
    return neo_db.NeoDB(progress_monitor=progress_monitor)


@pytest.fixture(scope="session")
def redis_db() -> RedisDB:
    return RedisDB()


@pytest.fixture(scope="session")
def redis_driver(redis_db):
    return redis_db.get_driver()


@pytest.fixture(scope="session")
def ingestion_manager(config_helper, multi_helper, progress_monitor):
    return IngestionManager(config_helper=config_helper, multi_helper=multi_helper, progress_monitor=progress_monitor)


@pytest.fixture(scope="session")
def spark_helper(config_helper) -> DevSparkHelper:
    return DevSparkHelper(config=config_helper)


@pytest.fixture(scope="session")
def nodes(config_helper):
    nodes = [
            {
                    config_helper.uid_property: i,
                    'name': f'{config_helper.test_labels[0]}{i}',
                    'age': i,
                    'email': f'{config_helper.test_labels[0]}{i}@gmail.com'
            }
            for i in range(0, config_helper.number_of_test_nodes)
    ]
    return nodes


@pytest.fixture(scope="session")
def edges(config_helper):
    edges = [
            {
                    config_helper.from_uid_property: i,
                    config_helper.to_uid_property: i * 2,
                    config_helper.edge_type_property: config_helper.test_edge_type
            }
            for i in range(0, config_helper.number_of_test_edges)
    ]
    return edges


@pytest.fixture(scope="session")
def logger() -> Logger:
    return log_helper.get_logger('Testing-Suite', )


@pytest.fixture(scope="session")
def data_and_model_provider() -> MockDataAndModelProvider:
    return MockDataAndModelProvider()


@pytest.fixture(scope="session")
def data_to_graph_entities_provider() -> MockDataToGraphEntitiesProvider:
    return MockDataToGraphEntitiesProvider()


# noinspection PyBroadException
def validate_front_desk_is_serving() -> bool:
    try:
        _ = requests.get(commons.config.test_front_desk_address + ':9001/redis')
    except Exception as _:
        return False
    return True


def start_front_desk():
    runpy.run_module(mod_name='giraffe.business_logic.front_desk', run_name='__main__')


@pytest.fixture(scope="session", autouse=True)
def init_and_finalize(redis_db, neo, logger, config_helper):
    commons.bootstrap()

    front_desk_process = Process(target=start_front_desk)
    front_desk_process.start()
    is_front_end_up = timing_utils.wait_for(condition=validate_front_desk_is_serving,
                                            condition_name='Front-Desk',
                                            sec_sleep=1,
                                            retries=20,
                                            logger=logger)
    if not is_front_end_up:
        pytest.fail('Failed starting front-end service.')

    yield  # Commands beyond this line will be called after the last test
    front_desk_process.terminate()
    front_desk_process.join()
    # noinspection PyProtectedMember
    neo._driver.close()
    redis_db.get_driver().close()
    log_helper.stop_listener()
