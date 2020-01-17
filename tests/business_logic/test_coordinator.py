import giraffe.configuration.common_testing_artifactrs as commons
import pytest
import requests

import tests.utilities.timing_utilities as timing_utils


def test_integration(ingestion_endpoint, config_helper, logger, white_list_file_path):
    commons.purge_redis_database()
    commons.purge_neo4j_database()
    request_body = {
            'request_id': 'my_request_id',
            'request_type': 'white_list',
            'file_path': white_list_file_path
    }
    logger.info(f'Requesting the ingestion to begin with a path to a white-list at : {white_list_file_path}.')
    reply = requests.post(ingestion_endpoint, json=request_body)
    logger.info(f'Ingestion request sent and replied with code of {reply.status_code}, text: {reply.text}')
    assert reply.status_code == 200

    secs = 1
    waiting_iterations = 90

    def nodes_found_in_neo4j() -> bool:
        query = 'MATCH (n:MockPerson) RETURN COUNT(*) AS count'
        count = commons.neo.pull_query(query=query).value()[0]
        return count == 20  # 10 for each of (source-1, source-2)

    expected_nodes_found_in_neo = timing_utils.wait_for(condition=nodes_found_in_neo4j,
                                                        condition_name='Nodes-In-Neo4j',
                                                        sec_sleep=secs,
                                                        retries=waiting_iterations,
                                                        logger=logger)
    if not expected_nodes_found_in_neo:
        pytest.fail(f'Neo4j does not seem to contain the expected nodes after {secs * waiting_iterations} seconds.')

    logger.info('Cool — values are finally in Neo4j.')
