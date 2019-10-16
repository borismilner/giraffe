from logging import Logger

from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.graph_db.neo_db import NeoDB
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.tools.redis_db import RedisDB
from redis import Redis

config = ConfigHelper()

log: Logger = log_helper.get_logger(logger_name='testing_redis')
neo: NeoDB = NeoDB(ConfigHelper())
redis_db: RedisDB = RedisDB(ConfigHelper())
redis_driver: Redis = redis_db.get_driver()
ingestion_manager: IngestionManager = IngestionManager()


def delete_redis_test_data():
    global log, redis_db, redis_driver, ingestion_manager
    db: RedisDB = redis_db
    db.purge_all()


def delete_neo_test_data():
    label_to_delete = config.test_labels[0]
    log.debug(f'Purging all nodes with label: {label_to_delete}')
    query = f'MATCH (node:{label_to_delete}) DETACH DELETE node'
    summary = neo.run_query(query=query)
    log.debug(f'Removed {summary.counters.nodes_deleted} {label_to_delete} nodes.')


def init_test_data():
    global log, redis_db, redis_driver, ingestion_manager
    im: IngestionManager = ingestion_manager

    # Populate nodes
    im.populate_job(job_name=config.test_job_name,
                    operation_required='nodes_ingest',
                    operation_arguments=','.join(config.test_labels),
                    items=[str(value) for value in test_nodes])

    # Populate edges
    im.populate_job(job_name=config.test_job_name,
                    operation_required='edges_ingest',
                    operation_arguments=f'{config.test_edge_type},{config.test_labels[0]},{config.test_labels[0]}',
                    items=[str(value) for value in test_edges])


test_nodes = [
    {
        '_uid': i,
        'name': f'person{i}',
        'age': i,
        'email': f'person{i}@gmail.com'
    }
    for i in range(0, config.number_of_test_nodes)
]

test_edges = [
    {
        '_fromUid': i,
        '_toUid': i * 2,
        '_edgeType': config.test_edge_type
    }
    for i in range(0, config.number_of_test_edges)
]
