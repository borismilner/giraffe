from giraffe.business_logic.ingestion_manger import IngestionManager
import giraffe.configuration.common_testing_artifactrs as commons
from giraffe.helpers.config_helper import ConfigHelper
from redis import Redis

config = ConfigHelper()


def test_populate_job():
    r: Redis = commons.redis_driver
    im: IngestionManager = commons.ingestion_manager

    commons.delete_redis_test_data()

    # Populate nodes
    im.populate_job(job_name=config.test_job_name,
                    operation_required='nodes_ingest',
                    operation_arguments=','.join(config.test_labels),
                    items=[str(value) for value in commons.test_nodes])

    # Populate edges
    im.populate_job(job_name=config.test_job_name,
                    operation_required='edges_ingest',
                    operation_arguments=f'{config.test_edge_type},{config.test_labels[0]}',
                    items=[str(value) for value in commons.test_edges])

    keys = r.keys(pattern=f'{config.test_job_name}*')
    assert len(keys) == 2
    node_keys = r.keys(pattern=f'{config.test_job_name}:{config.nodes_ingestion_operation}:*')
    assert len(node_keys) == 1
    edges_keys = r.keys(pattern=f'{config.test_job_name}:{config.edges_ingestion_operation}:*')
    assert len(edges_keys) == 1

    nodes_key = node_keys[0].decode('utf8')
    edges_key = edges_keys[0].decode('utf8')

    num_stored_nodes = r.scard(name=nodes_key)
    assert num_stored_nodes == len(commons.test_nodes)
    num_stored_edges = r.scard(name=edges_key)
    assert num_stored_edges == len(commons.test_edges)


def test_pull_job_from_redis_to_neo():
    commons.delete_redis_test_data()
    commons.init_test_data()
    im = commons.IngestionManager()
    im.pull_job_from_redis_to_neo(job_name=config.test_job_name)