import logging
import pickle
from logging import Logger
from multiprocessing import Manager
from multiprocessing import Queue

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.data_access import neo_db
from giraffe.data_access.neo_db import NeoDB
from giraffe.data_access.redis_db import RedisDB
from giraffe.helpers.config_helper import ConfigHelper
from redis import Redis

config = ConfigHelper()
queue_for_logging: Queue

log: Logger
neo: NeoDB
redis_db: RedisDB
r: Redis
ingestion_manager: IngestionManager
elastic_search: Elasticsearch


def bootstrap():
    global log, neo, redis_db, r, ingestion_manager, elastic_search, queue_for_logging
    queue_for_logging = Manager().Queue(-1)
    log = logging.getLogger('testing_redis')
    neo = NeoDB(config=ConfigHelper())
    redis_db = RedisDB(config=ConfigHelper())
    r = redis_db.get_driver()
    ingestion_manager = IngestionManager(config_helper=config)
    elastic_search = Elasticsearch()


def purge_redis_database():
    global log, redis_db, r, ingestion_manager
    db: RedisDB = redis_db
    logger: Logger = log
    logger.info("Purging all keys from redis.")
    db.purge_all()


def purge_neo4j_database():
    label_to_delete = config.test_labels[0]
    log.debug(f'Purging all nodes with label: {label_to_delete}')
    query = f'MATCH (n) DETACH DELETE n'
    summary = neo.run_query(query=query)
    log.debug(f'Removed {summary.counters.nodes_deleted} {label_to_delete} nodes.')


def purge_elasticsearch_database():
    global elastic_search, redis_db, r, ingestion_manager
    es: Elasticsearch = elastic_search
    test_index = config.test_elasticsearch_index
    log.debug(f'Purging ES test index: {test_index}')
    es.indices.delete(index=test_index, ignore=[400, 404])


def delete_redis_keys_prefix(prefix: str) -> int:
    redis_driver = redis_db.get_driver()
    num_deleted = 0
    log.debug(f'Deleting keys with a prefix of: {prefix}')
    for key in redis_driver.scan_iter(prefix):
        redis_driver.delete(key)
        num_deleted += 1
    log.debug(f'Deleted {num_deleted} keys from Redis.')
    return num_deleted


def init_elastic_test_data():
    global elastic_search, log, redis_db, r, ingestion_manager
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
    global log, redis_db, r, ingestion_manager
    im: IngestionManager = ingestion_manager

    # Populate nodes
    im.publish_job(job_name=config.test_job_name,
                   operation='nodes_ingest',
                   operation_arguments=config.test_labels[0],
                   items=[pickle.dumps(value).hex() for value in test_nodes])

    # Populate edges
    im.publish_job(job_name=config.test_job_name,
                   operation='edges_ingest',
                   operation_arguments=f'{config.test_edge_type},{config.test_labels[0]},{config.test_labels[0]}',
                   items=[pickle.dumps(value).hex() for value in test_edges])


def init_neo_test_data():
    global neo
    db: neo_db.NeoDB = neo
    db.merge_nodes(nodes=test_nodes, label=config.test_labels[0])
    db.create_index_if_not_exists(label=config.test_labels[0], property_name='_uid')


test_nodes = [
        {
                config.uid_property: i,
                'name': f'{config.test_labels[0]}{i}',
                'age': i,
                'email': f'{config.test_labels[0]}{i}@gmail.com'
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
