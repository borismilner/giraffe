from redis import Redis
from giraffe.helpers.config_helper import ConfigHelper
import giraffe.configuration.common_testing_artifactrs as commons
from giraffe.business_logic.ingestion_manger import IngestionManager

config = ConfigHelper()


def test_parse_redis_key():
    im = IngestionManager()
    job_name = config.nodes_ingestion_operation
    operation = config.nodes_ingestion_operation
    labels = config.test_labels
    parsed: IngestionManager.key_elements_type = im.parse_redis_key(
        key=f'{job_name}{config.key_separator}{operation}{config.key_separator}{",".join(labels)}')
    assert parsed.job_name == job_name
    assert parsed.operation == operation
    assert set(parsed.arguments) == set(labels)


def test_publish_job():
    r: Redis = commons.redis_driver
    im: IngestionManager = commons.ingestion_manager

    commons.delete_redis_test_data()

    # Populate nodes
    im.publish_job(job_name=config.test_job_name,
                   operation=config.nodes_ingestion_operation,
                   operation_arguments=','.join(config.test_labels),
                   items=[str(value) for value in commons.test_nodes])

    # Populate edges
    im.publish_job(job_name=config.test_job_name,
                   operation=config.edges_ingestion_operation,
                   operation_arguments=f'{config.test_edge_type},{config.test_labels[0]}',
                   items=[str(value) for value in commons.test_edges])

    keys = r.keys(pattern=f'{config.test_job_name}*')
    assert len(keys) == 2
    node_keys = r.keys(pattern=f'{config.test_job_name}{config.key_separator}{config.nodes_ingestion_operation}{config.key_separator}*')
    assert len(node_keys) == 1
    edges_keys = r.keys(pattern=f'{config.test_job_name}{config.key_separator}{config.edges_ingestion_operation}{config.key_separator}*')
    assert len(edges_keys) == 1

    nodes_key = node_keys[0].decode(config.string_encoding)
    edges_key = edges_keys[0].decode(config.string_encoding)

    num_stored_nodes = r.scard(name=nodes_key)
    assert num_stored_nodes == len(commons.test_nodes)
    num_stored_edges = r.scard(name=edges_key)
    assert num_stored_edges == len(commons.test_edges)


def test_process_job():
    commons.delete_neo_test_data()
    commons.delete_redis_test_data()
    commons.init_test_data()
    im = commons.IngestionManager()
    im.process_job(job_name=config.test_job_name)
