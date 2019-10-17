import os
import collections
from typing import List

from giraffe.exceptions.logical import MissingKeyError, UnexpectedOperation
from giraffe.exceptions.technical import TechnicalError
from giraffe.data_access.neo_db import NeoDB
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.data_access.redis_db import RedisDB
from redis import Redis


class IngestionManager:
    key_elements_type = collections.namedtuple('key_elements_type', 'job_name operation arguments')

    supported_operations: List[str]

    def __init__(self, config_file_path: str = ConfigHelper.default_configurations_file):
        if not os.path.isfile(config_file_path):
            raise TechnicalError(f'Configuration file {config_file_path} does not exist.')
        self.config = ConfigHelper(configurations_ini_file_path=config_file_path)
        self.neo_db: NeoDB = NeoDB(config=self.config)
        self.redis_db: RedisDB = RedisDB(config=self.config)
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        IngestionManager.supported_operations = (self.config.nodes_ingestion_operation, self.config.edges_ingestion_operation)

    @staticmethod
    def validate_operation(operation_name: str):
        if operation_name not in IngestionManager.supported_operations:
            raise UnexpectedOperation(f'Operation {operation_name} is not supported. (supported: {IngestionManager.supported_operations})')

    @staticmethod
    def validate_job_name(job_name: str):
        if ':' in job_name:
            raise TechnicalError(f'Job name {job_name} must not contain colons (it is used internally...)')

    @staticmethod
    def order_jobs(element):
        # Order of the jobs --> <nodes> before <edges> --> Batches sorted by [batch-number] ascending.
        # noinspection PyRedundantParentheses
        return 'a' if 'nodes' in element else 'z'

    def publish_job(self, job_name: str, operation: str, operation_arguments: str, items: List):
        IngestionManager.validate_operation(operation)
        r: Redis = self.redis_db.driver
        result = r.sadd(f'{job_name}:{operation}:{operation_arguments}', *items)
        assert result == len(items) or result == 0

    def parse_redis_key(self, key: str) -> key_elements_type:
        expected_parts_num = 3
        key_parts = key.split(self.config.key_separator)
        parts_count = len(key_parts)
        if parts_count != expected_parts_num:
            raise TechnicalError(f'Expected {expected_parts_num} parts in {key} but got {parts_count}')
        if len(key_parts[0]) == 0:
            raise TechnicalError(f'Job name must not be empty ! [{key}]')
        job_name = key_parts[0]
        IngestionManager.validate_job_name(job_name=job_name)
        operation = key_parts[1]
        IngestionManager.validate_operation(operation_name=operation)

        arguments = key_parts[2].split(',')
        # noinspection PyCallByClass
        return IngestionManager.key_elements_type(job_name=job_name, operation=operation, arguments=arguments)

    def process_job(self, job_name: str, batch_size: int = 50_000):

        keys_found = self.redis_db.get_key_by_pattern(key_pattern=f'{job_name}{self.config.key_separator}*')
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
                    self.push_to_neo(awaiting_jobs, is_nodes, jobs, key)
                    awaiting_jobs = 0
            if len(jobs) > 0:
                self.push_to_neo(awaiting_jobs, is_nodes, jobs, key)

    def push_to_neo(self, awaiting_jobs, is_nodes, jobs, key):
        key_parts = self.parse_redis_key(key=key)
        self.log.info(f'Placing {awaiting_jobs} {"nodes" if is_nodes else "edges"} into Neo4j')
        jobs = [eval(job) for job in jobs]
        if is_nodes:
            self.neo_db.merge_nodes(nodes=jobs,
                                    label=str(key_parts.arguments[0]))  # TODO: Adjust for multiple labels
        else:
            self.neo_db.merge_edges(edges=jobs,
                                    from_label=key_parts.arguments[1],
                                    to_label=key_parts.arguments[2],
                                    edge_type=key_parts.arguments[0])
        jobs.clear()
