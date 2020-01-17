import pickle
import collections
from typing import List

from giraffe.data_access.neo_db import NeoDB
from giraffe.data_access.redis_db import RedisDB
from giraffe.exceptions.logical import MissingKeyError
from giraffe.exceptions.logical import UnexpectedOperation
from giraffe.exceptions.technical import TechnicalError
from giraffe.helpers import log_helper
from giraffe.helpers import utilities
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.helpers.multi_helper import MultiHelper
from giraffe.monitoring.progress_monitor import ProgressMonitor
from redis import Redis


class IngestionManager:
    key_elements_type = collections.namedtuple('key_elements_type', ['job_name', 'operation', 'arguments'])

    supported_operations: List[str]

    def __init__(self, config_helper: ConfigHelper, multi_helper: MultiHelper, progress_monitor: ProgressMonitor):
        self.is_ready = False
        self.progress_monitor: ProgressMonitor = progress_monitor
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.config = config_helper
        try:
            self.neo_db: NeoDB = NeoDB(config=self.config, progress_monitor=self.progress_monitor)
            self.redis_db: RedisDB = RedisDB(config=self.config)
            self.multi_helper: MultiHelper = multi_helper
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

    def process_redis_content(self, request_id: str, translation_id: str, batch_size: int = 50_000):

        # self.progress_monitor.processing_redis_content(request_id=request_id, key_prefix=key_prefix, redis_db=self.redis_db)
        keys_found = self.redis_db.get_key_by_pattern(key_pattern=f'{translation_id}{self.config.key_separator}*')
        if len(keys_found) == 0:
            raise MissingKeyError(f'No redis keys with a prefix of: {translation_id}.')

        # Handles nodes before edges
        keys_found.sort(key=lambda item: (IngestionManager.order_jobs(item), str.lower(item)))

        all_futures = []
        for key in keys_found:
            is_nodes = 'nodes' in key
            iterator = self.redis_db.pull_set_members_in_batches(key_pattern=key,
                                                                 batch_size=batch_size)

            for batch in utilities.iterable_in_batches(iterable=iterator, batch_size=batch_size):
                future = self.push_to_neo(entries=batch,
                                          is_nodes=is_nodes,
                                          key=key,
                                          request_id=request_id)
                all_futures.append(future)

            parallel_results = MultiHelper.wait_on_futures(iterable=all_futures)
            for exception in parallel_results.exceptions:
                self.progress_monitor.error(request_id=request_id,
                                            message='Failed pushing into neo4j',
                                            exception=exception)
            all_futures.clear()

    def push_to_neo(self, is_nodes, entries, key, request_id: str, needs_eval=True):  # needs_eval must be True when jobs are strings (and not dicts)
        key_parts = self.parse_redis_key(key=key)
        elements_count = len(entries)

        self.progress_monitor.pushing_elements_into_neo4j(key=key,
                                                          request_id=request_id,
                                                          how_many=elements_count)

        if needs_eval:
            entries = [pickle.loads(bytes.fromhex(job)) for job in entries]
        if is_nodes:
            future = self.multi_helper.run_in_separate_thread(function=self.neo_db.merge_nodes,
                                                              nodes=entries,
                                                              label=str(key_parts.arguments[0]),
                                                              request_id=request_id
                                                              )
        else:
            future = self.multi_helper.run_in_separate_thread(function=self.neo_db.merge_edges,
                                                              edges=entries,
                                                              from_label=key_parts.arguments[1],
                                                              to_label=key_parts.arguments[2],
                                                              edge_type=key_parts.arguments[0],
                                                              request_id=request_id
                                                              )
        return future
