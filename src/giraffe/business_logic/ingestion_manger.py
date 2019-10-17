import os
import collections
from typing import List

from giraffe.exceptions.logical import MissingKeyError, UnexpectedOperation
from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db.neo_db import NeoDB
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.tools.redis_db import RedisDB
from redis import Redis


class IngestionManager:
    key_elements_type = collections.namedtuple('key_elements_type', 'job_name operation arguments')

    def __init__(self, config_file_path: str = ConfigHelper.default_configurations_file):
        if not os.path.isfile(config_file_path):
            raise TechnicalError(f'Configuration file {config_file_path} does not exist.')
        self.config = ConfigHelper(configurations_ini_file_path=config_file_path)
        self.neo_db: NeoDB = NeoDB(config=self.config)
        self.redis_db: RedisDB = RedisDB(config=self.config)
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.supported_operations = (
            self.config.nodes_ingestion_operation,
            self.config.edges_ingestion_operation
        )

    @staticmethod
    def order_jobs(element):
        # Order of the jobs --> <nodes> before <edges> --> Batches sorted by [batch-number] ascending.
        # noinspection PyRedundantParentheses
        return 'a' if 'nodes' in element else 'z'

    def parse_redis_key(self, key: str) -> key_elements_type:
        expected_parts_num = 3
        key_parts = key.split(self.config.key_separator)
        parts_count = len(key_parts)
        if parts_count != expected_parts_num:
            raise TechnicalError(f'Expected {expected_parts_num} parts in {key} but got {parts_count}')
        if len(key_parts[0]) == 0:
            raise TechnicalError(f'Job name must not be empty ! [{key}]')
        job_name = key_parts[0]
        operation = key_parts[1]

        if operation not in self.supported_operations:
            raise UnexpectedOperation(f'Operation {operation} is not supported. (supported: {self.supported_operations})')

        arguments = key_parts[2].split(',')
        # noinspection PyCallByClass
        return IngestionManager.key_elements_type(job_name=job_name, operation=operation, arguments=arguments)

    def populate_job(self, job_name: str, operation_required: str, operation_arguments: str, items: List):
        r: Redis = self.redis_db.driver
        result = r.sadd(f'{job_name}:{operation_required}:{operation_arguments}', *items)
        assert result == len(items) or result == 0

    def pull_job_from_redis_to_neo(self, job_name: str, batch_size: int = 50_000):

        keys_found = self.redis_db.get_key_by_pattern(key_pattern=f'{job_name}:*')
        if len(keys_found) != 2:
            raise MissingKeyError(f'Could not find expected keys for job: {job_name}.')  # TODO: More informative message

        # Handles nodes before edges
        keys_found.sort(key=IngestionManager.order_jobs)

        # Nodes
        for i, key in enumerate(keys_found):
            is_nodes = i == 0
            iterator = self.redis_db.pull_in_batches(key_pattern=key, batch_size=batch_size)
            awaiting_jobs = 0
            jobs = []
            for job in iterator:
                jobs.append(job.decode('utf8'))
                awaiting_jobs += 1
                if awaiting_jobs >= batch_size:
                    self.push_no_neo(awaiting_jobs, is_nodes, jobs, key)
                    awaiting_jobs = 0
            if len(jobs) > 0:
                self.push_no_neo(awaiting_jobs, is_nodes, jobs, key)

    def push_no_neo(self, awaiting_jobs, is_nodes, jobs, key):
        self.log.info(f'Placing {awaiting_jobs} {"nodes" if is_nodes else "edges"} into Neo4j')
        arguments = key.split(':')[2].split(',')
        jobs = [eval(job) for job in jobs]
        if is_nodes:
            self.neo_db.merge_nodes(nodes=jobs, label=arguments[0])  # TODO: Adjust for multiple labels
        else:
            self.neo_db.merge_edges(edges=jobs, from_label=arguments[1], to_label=arguments[2], edge_type=arguments[0])
        jobs.clear()
