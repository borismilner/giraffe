import giraffe.configuration.common_testing_artifactrs as commons
import pytest
import requests
from giraffe.helpers.communicator import Communicator
from giraffe.helpers.communicator import CommunicatorMode
from giraffe.monitoring.giraffe_event import GiraffeEventType

import tests.utilities.timing_utilities as timing_utils


def test_integration(ingestion_endpoint, config_helper, logger, white_list_file_path, redis_db, neo):
    monitor = Communicator(mode=CommunicatorMode.CLIENT)
    commons.purge_redis_database(redis_db=redis_db, log=logger)
    commons.purge_neo4j_database(log=logger, neo=neo)

    request_id = 'my_request_id'

    request_body = {
            'request_id': request_id,
            'request_type': 'white_list',
            'file_path': white_list_file_path
    }
    logger.info(f'Requesting the ingestion to begin with a path to a white-list at : {white_list_file_path}.')
    reply = requests.post(ingestion_endpoint, json=request_body)
    logger.info(f'Ingestion request sent and replied with code of {reply.status_code}, text: {reply.text}')
    assert reply.status_code == 200

    events = monitor.events_iterator(request_id=request_id, timeout_seconds=90)

    events_count = 0
    for index, event in enumerate(events):
        events_count += 1
        logger.info(f'Received event: {event.event_type}')
        if index == 0:
            assert request_id in event.request_id
            assert event.event_type == GiraffeEventType.RECEIVED_REQUEST
            assert 'client_ip' in event.arguments.keys()
            assert 'request_content' in event.arguments.keys()
        elif index == 1:
            assert event.event_type == GiraffeEventType.STARTED_PROCESSING_REQUEST
            assert request_id in event.request_id
            assert 'request_type' in event.arguments.keys()
            assert 'request_content' in event.arguments.keys()
            assert request_id in event.message
        elif index == 2:
            assert event.event_type == GiraffeEventType.FETCHING_DATA_AND_MODELS
            assert request_id in event.request_id
            assert 'request_id' in event.arguments.keys()
            assert 'source_description' in event.arguments.keys()
        elif index == 3:
            assert event.event_type == GiraffeEventType.FINISHED_FETCHING_DATA_AND_MODELS
            assert 'request_id' in event.arguments.keys()
        elif index == 4 or index == 5:
            assert event.event_type == GiraffeEventType.WRITING_GRAPH_ELEMENTS_INTO_REDIS
            assert request_id in event.request_id
            assert 'request_id' in event.arguments.keys()
            assert 'source_name' in event.arguments.keys()
        elif index == 6:
            assert event.event_type == GiraffeEventType.REDIS_IS_READY_FOR_CONSUMPTION
            assert request_id in event.request_id
            assert 'request_id' in event.arguments.keys()
            assert 'parallel_results' in event.arguments.keys()
        elif index == 7 or index == 10:
            assert event.event_type == GiraffeEventType.WRITING_FROM_REDIS_TO_NEO
        elif index == 8 or index == 11:
            assert event.event_type == GiraffeEventType.PUSHED_GRAPH_ELEMENTS_INTO_NEO
            assert request_id in event.request_id
        elif index == 9 or index == 12:
            assert event.event_type == GiraffeEventType.DELETING_REDIS_KEYS
            assert request_id in event.request_id
            assert 'request_id' in event.arguments.keys()
            assert 'keys' in event.arguments.keys()
            monitor.set_client_timeout_seconds(30)
        elif index == 13:
            assert event.event_type == GiraffeEventType.DONE_PROCESSING_REQUEST
            assert request_id in event.request_id
            assert 'request_id' in event.arguments.keys()
    assert events_count == 14

    secs = 1
    waiting_iterations = 90

    def nodes_found_in_neo4j() -> bool:
        query = 'MATCH (n:MockPerson) RETURN COUNT(*) AS count'
        count = neo.pull_query(query=query).value()[0]
        return count == 20  # 10 for each of (source-1, source-2)

    expected_nodes_found_in_neo = timing_utils.wait_for(condition=nodes_found_in_neo4j,
                                                        condition_name='Nodes-In-Neo4j',
                                                        sec_sleep=secs,
                                                        retries=waiting_iterations,
                                                        logger=logger)
    if not expected_nodes_found_in_neo:
        pytest.fail(f'Neo4j does not seem to contain the expected nodes after {secs * waiting_iterations} seconds.')

    logger.info('Cool — values are finally in Neo4j.')
