from logging import Logger
from time import sleep

import giraffe.configuration.common_testing_artifactrs as commons
import pytest
from giraffe.data_access import neo_db
from giraffe.helpers import log_helper
from giraffe.helpers.utilities import Timer


def prepare_neo(config_helper, how_many_to_delete: int = 1_000_000):
    commons.purge_neo4j_database()
    neo = commons.neo
    db: neo_db.NeoDB = neo

    test_nodes = [
            {
                    config_helper.uid_property: i,
                    'name': 'Arafat',
                    'age': i,
                    'email': f'{config_helper.test_labels[0]}{i}@gmail.com'
            }
            for i in range(0, how_many_to_delete)
    ]

    db.merge_nodes(nodes=test_nodes, label=config_helper.test_labels[0])
    db.create_index_if_not_exists(label=config_helper.test_labels[0], property_name='_uid')
    db.create_index_if_not_exists(label=config_helper.test_labels[0], property_name='name')


@pytest.mark.skip(reason="Not really a test, more of a timing estimation")
def test_delete_nodes_by_property(config_helper):
    prepare_neo(config_helper=config_helper)
    log: Logger = log_helper.get_logger(logger_name='Timing...')

    neo = commons.neo
    db: neo_db.NeoDB = neo
    test_label = config_helper.test_labels[0]
    sleep(3)

    query = f"MATCH(n:{test_label}) return count(n) as count"
    before_deletion_count = db.pull_query(query=query).value()[0]
    log.info(f'Before deletion there are {before_deletion_count} items.')
    timer = Timer()
    timer.start()
    result: dict = db.delete_nodes_by_properties(label=test_label, property_name='name', property_value='Arafat')
    sec = timer.stop()
    log.info(f'Time elapsed: {sec} seconds')
    after_deletion_count = db.pull_query(query=query).value()[0]
    log.info(f'After deletion there are {after_deletion_count} items.')
    deleted_count = result['total']
    assert deleted_count > 0
    assert before_deletion_count - after_deletion_count == deleted_count
