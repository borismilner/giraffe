import os
import collections
from typing import List

from giraffe.exceptions.logical import MissingKeyError, UnexpectedOperation
from giraffe.exceptions.technical import TechnicalError
from giraffe.data_access.neo_db import NeoDB
from giraffe.helpers import log_helper
from giraffe.helpers.utilities import iterable_in_chunks
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

    def validate_job_name(self, job_name: str):
        if self.config.key_separator in job_name:
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
        self.validate_job_name(job_name=job_name)
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
            iterator = self.redis_db.pull_set_members_in_batches(key_pattern=key, batch_size=batch_size)
            awaiting_jobs = 0
            jobs = []
            for job in iterator:
                jobs.append(job)
                awaiting_jobs += 1
                if awaiting_jobs >= batch_size:
                    self.push_to_neo(awaiting_jobs, is_nodes, jobs, key)
                    awaiting_jobs = 0
            if len(jobs) > 0:
                self.push_to_neo(awaiting_jobs, is_nodes, jobs, key)

    # ---------------------------------------------------------------------------------------------------------------------------------------------#
    # This is an experimental feature, dealing with a redis-format dictated by the spark-redis "table" option.
    # This feature assumes the _uid field is to be taken from the final part of the key for each graph-element
    def process_spark_redis_table(self, job_name: str, batch_size: int = 50_000):

        node_keys = self.redis_db.get_key_by_pattern(key_pattern=f'{job_name}{self.config.key_separator}{self.config.nodes_ingestion_operation}*')
        edges_keys = self.redis_db.get_key_by_pattern(key_pattern=f'{job_name}{self.config.key_separator}{self.config.edges_ingestion_operation}*')

        for i, element_keys in enumerate((node_keys, edges_keys)):
            is_nodes = i == 0

            element_key = None
            for keys in iterable_in_chunks(iterable=element_keys, chunk_size=batch_size):
                jobs = self.redis_db.pull_batch_hashes_by_keys(keys=keys)
                # Planting the key inside the value-body
                for index, key in enumerate(keys):
                    if not element_key:
                        element_key = ''.join(key.split(self.config.key_separator)[0:-1])
                    jobs[index][self.config.uid_property] = key.split(self.config.key_separator)[-1]

                self.push_to_neo(awaiting_jobs=len(jobs), is_nodes=is_nodes, jobs=jobs, key=element_key, needs_eval=False)

    # ---------------------------------------------------------------------------------------------------------------------------------------------#

    def push_to_neo(self, awaiting_jobs, is_nodes, jobs, key, needs_eval=True):  # needs_eval must be True when jobs are strings (and not dicts)
        key_parts = self.parse_redis_key(key=key)
        self.log.info(f'Placing {awaiting_jobs} {"nodes" if is_nodes else "edges"} into Neo4j')
        if needs_eval:
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
