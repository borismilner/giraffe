from logging import Logger

from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.data_access import neo_db
from giraffe.data_access.neo_db import NeoDB
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.data_access.redis_db import RedisDB
from elasticsearch import Elasticsearch, helpers
from redis import Redis

config = ConfigHelper()

log: Logger = log_helper.get_logger(logger_name='testing_redis')
neo: NeoDB = NeoDB(ConfigHelper())
redis_db: RedisDB = RedisDB(ConfigHelper())
redis_driver: Redis = redis_db.get_driver()
ingestion_manager: IngestionManager = IngestionManager()
elastic_search: Elasticsearch = Elasticsearch()


def delete_redis_test_data():
    global log, redis_db, redis_driver, ingestion_manager
    db: RedisDB = redis_db
    logger: Logger = log
    logger.info("Emptying redis.")
    db.purge_all()


def delete_neo_test_data():
    label_to_delete = config.test_labels[0]
    log.debug(f'Purging all nodes with label: {label_to_delete}')
    query = f'MATCH (node:{label_to_delete}) DETACH DELETE node'
    summary = neo.run_query(query=query)
    log.debug(f'Removed {summary.counters.nodes_deleted} {label_to_delete} nodes.')


def delete_elastic_test_data():
    global elastic_search, redis_db, redis_driver, ingestion_manager
    es: Elasticsearch = elastic_search
    test_index = config.test_elasticsearch_index
    log.debug(f'Purging ES test index: {test_index}')
    es.indices.delete(index=test_index, ignore=[400, 404])


def delete_redis_keys_prefix(prefix: str) -> int:
    r = redis_db.get_driver()
    num_deleted = 0
    log.debug(f'Deleting keys with a prefix of: {prefix}')
    for key in r.scan_iter(prefix):
        r.delete(key)
        num_deleted += 1
    log.debug(f'Deleted {num_deleted} keys from Redis.')
    return num_deleted


def init_elastic_test_data():
    global elastic_search, log, redis_db, redis_driver, ingestion_manager
    es: Elasticsearch = elastic_search
    actions = []
    for node in test_nodes:
        actions.append(
            {
                "_index": config.test_elasticsearch_index,
                "_source": node
            }
        )
    helpers.bulk(es, actions, refresh=True)


def init_redis_test_data():
    global log, redis_db, redis_driver, ingestion_manager
    im: IngestionManager = ingestion_manager

    # Populate nodes
    im.publish_job(job_name=config.test_job_name,
                   operation='nodes_ingest',
                   operation_arguments=config.test_labels[0],
                   items=[str(value) for value in test_nodes])

    # Populate edges
    im.publish_job(job_name=config.test_job_name,
                   operation='edges_ingest',
                   operation_arguments=f'{config.test_edge_type},{config.test_labels[0]},{config.test_labels[0]}',
                   items=[str(value) for value in test_edges])


def init_test_data():
    global neo
    db: neo_db.NeoDB = neo
    db.merge_nodes(nodes=test_nodes, label=config.test_labels[0])


test_nodes = [
    {
        config.uid_property: i,
        'name': f'person{i}',
        'age': i,
        'email': f'person{i}@gmail.com'
    }
    for i in range(0, config.number_of_test_nodes)
]

test_edges = [
    {
        config.from_uid_property: i,
        config.to_uid_property: i * 2,
        config.edge_type_property: config.test_edge_type
    }
    for i in range(0, config.number_of_test_edges)
]
