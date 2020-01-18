import giraffe.configuration.common_testing_artifactrs as commons
from giraffe.business_logic.ingestion_manger import IngestionManager
from redis import Redis


def test_parse_redis_key(config_helper, ingestion_manager):
    im = ingestion_manager
    job_name = config_helper.nodes_ingestion_operation
    operation = config_helper.nodes_ingestion_operation
    labels = config_helper.test_labels
    parsed: IngestionManager.key_elements_type = im.parse_redis_key(
            key=f'{job_name}{config_helper.key_separator}{operation}{config_helper.key_separator}{",".join(labels)}')
    assert parsed.job_name == job_name
    assert parsed.operation == operation
    assert set(parsed.arguments) == set(labels)


def test_publish_job(config_helper, redis_driver, ingestion_manager, nodes, edges):
    r: Redis = redis_driver
    im: IngestionManager = ingestion_manager

    commons.purge_redis_database()

    # Populate nodes
    im.publish_job(job_name=config_helper.test_job_name,
                   operation=config_helper.nodes_ingestion_operation,
                   operation_arguments=','.join(config_helper.test_labels),
                   items=[str(value) for value in nodes])

    # Populate edges
    im.publish_job(job_name=config_helper.test_job_name,
                   operation=config_helper.edges_ingestion_operation,
                   operation_arguments=f'{config_helper.test_edge_type},{config_helper.test_labels[0]}',
                   items=[str(value) for value in edges])

    keys = r.keys(pattern=f'{config_helper.test_job_name}*')
    assert len(keys) == 2
    node_keys = r.keys(pattern=f'{config_helper.test_job_name}{config_helper.key_separator}{config_helper.nodes_ingestion_operation}{config_helper.key_separator}*')
    assert len(node_keys) == 1
    edges_keys = r.keys(pattern=f'{config_helper.test_job_name}{config_helper.key_separator}{config_helper.edges_ingestion_operation}{config_helper.key_separator}*')
    assert len(edges_keys) == 1

    nodes_key = node_keys[0]
    edges_key = edges_keys[0]

    num_stored_nodes = r.scard(name=nodes_key)
    assert num_stored_nodes == len(nodes)
    num_stored_edges = r.scard(name=edges_key)
    assert num_stored_edges == len(edges)


def test_process_job(config_helper, ingestion_manager):
    commons.purge_redis_database()
    commons.purge_neo4j_database()
    commons.init_redis_test_data()
    im = ingestion_manager
    im.process_redis_content(translation_id=config_helper.test_job_name, request_id='unit-testing')
    query = f'MATCH (:{config_helper.test_labels[0]}) RETURN COUNT(*) AS count'
    count = commons.neo.pull_query(query=query).value()[0]
    assert count == config_helper.number_of_test_nodes
    query = f'MATCH ()-[:{config_helper.test_edge_type}]->() RETURN COUNT(*) AS count'
    count = commons.neo.pull_query(query=query).value()[0]
    assert count == config_helper.number_of_test_edges
