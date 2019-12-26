import collections
import pickle
from typing import List

from giraffe.data_access.neo_db import NeoDB
from giraffe.data_access.redis_db import RedisDB
from giraffe.exceptions.logical import MissingKeyError
from giraffe.exceptions.logical import UnexpectedOperation
from giraffe.exceptions.technical import TechnicalError
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.helpers.structured_logging_fields import Field
from redis import Redis


class IngestionManager:
    key_elements_type = collections.namedtuple('key_elements_type', 'job_name operation arguments')

    supported_operations: List[str]

    def __init__(self, config_helper: ConfigHelper):
        self.is_ready = False
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.config = config_helper
        try:
            self.neo_db: NeoDB = NeoDB(config=self.config)
            self.redis_db: RedisDB = RedisDB(config=self.config)
            self.is_ready = True
        except Exception as the_exception:
            self.log.error(the_exception, exc_info=True)
            self.is_ready = False
        IngestionManager.supported_operations = (
                self.config.nodes_ingestion_operation,
                self.config.edges_ingestion_operation
        )

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
        # return 'a' if 'nodes' in element else 'z'
        return 'nodes' not in element

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
        return IngestionManager.key_elements_type(job_name=job_name,
                                                  operation=operation,
                                                  arguments=arguments)

    def process_redis_content(self, key_prefix: str, request_id: str, batch_size: int = 50_000):

        keys_found = self.redis_db.get_key_by_pattern(key_pattern=f'{key_prefix}{self.config.key_separator}*')
        if len(keys_found) == 0:
            raise MissingKeyError(f'No redis keys with a prefix of: {key_prefix}.')

        # Handles nodes before edges
        keys_found.sort(key=lambda item: (IngestionManager.order_jobs(item), str.lower(item)))

        for key in keys_found:
            is_nodes = 'nodes' in key
            iterator = self.redis_db.pull_set_members_in_batches(key_pattern=key,
                                                                 batch_size=batch_size)
            awaiting_entries = 0
            entries = []
            for entry in iterator:
                entries.append(entry)
                awaiting_entries += 1
                if awaiting_entries < batch_size:
                    continue
                try:
                    self.push_to_neo(entries=entries,
                                     is_nodes=is_nodes,
                                     key=key,
                                     request_id=request_id)
                except Exception as the_exception:
                    self.log.error(the_exception,
                                   exc_info=True)

                entries.clear()
                awaiting_entries = 0

            if len(entries) > 0:  # Leftovers
                try:
                    self.push_to_neo(entries=entries,
                                     is_nodes=is_nodes,
                                     key=key,
                                     request_id=request_id)
                except Exception as the_exception:
                    self.log.error(the_exception,
                                   exc_info=True)
                entries.clear()

    def push_to_neo(self, is_nodes, entries, key, request_id: str, needs_eval=True):  # needs_eval must be True when jobs are strings (and not dicts)
        key_parts = self.parse_redis_key(key=key)
        elements_count = len(entries)
        elements_type = "nodes" if is_nodes else "edges"
        self.log.info(f'Pushing {elements_count} {elements_type} into Neo4j [{key}]')
        self.log.admin({
                Field.request_id: request_id,
                Field.writing_to_neo: elements_count,
                Field.element_type: elements_type
        })
        if needs_eval:
            entries = [pickle.loads(bytes.fromhex(job)) for job in entries]
        if is_nodes:
            self.neo_db.merge_nodes(nodes=entries,
                                    label=str(key_parts.arguments[0]))  # TODO: Adjust for multiple labels
        else:
            self.neo_db.merge_edges(edges=entries,
                                    from_label=key_parts.arguments[1],
                                    to_label=key_parts.arguments[2],
                                    edge_type=key_parts.arguments[0])
