import pickle
import collections
from typing import List

from giraffe.business_logic.env_provider import EnvProvider
from giraffe.data_access.neo_db import NeoDB
from giraffe.data_access.redis_db import RedisDB
from giraffe.exceptions.logical import MissingKeyError
from giraffe.exceptions.logical import UnexpectedOperation
from giraffe.exceptions.technical import TechnicalError
from giraffe.helpers import log_helper
from giraffe.helpers import utilities
from giraffe.helpers.EventDispatcher import EventDispatcher
from giraffe.helpers.multi_helper import MultiHelper
from giraffe.monitoring.giraffe_event import GiraffeEvent
from giraffe.monitoring.giraffe_event import GiraffeEventType
from redis import Redis


class IngestionManager:
    key_elements_type = collections.namedtuple('key_elements_type', ['job_name',
                                                                     'operation',
                                                                     'arguments'])

    supported_operations: List[str]

    def __init__(self,
                 env: EnvProvider,
                 multi_helper: MultiHelper,
                 event_dispatcher: EventDispatcher):
        self.is_ready = False
        self.event_dispatcher = event_dispatcher
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.config = env.config
        try:
            self.neo_db: NeoDB = NeoDB(config=self.config,
                                       event_dispatcher=self.event_dispatcher)
            self.redis_db: RedisDB = RedisDB(env=env, event_dispatcher=self.event_dispatcher)
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
            key_parts = self.parse_redis_key(key=key)
            iterator = self.redis_db.pull_set_members_in_batches(key_pattern=key,
                                                                 batch_size=batch_size)

            is_nodes = 'nodes' in key
            for batch in utilities.iterable_in_batches(iterable=iterator,
                                                       batch_size=batch_size):
                entries = [pickle.loads(bytes.fromhex(job)) for job in batch]

                if is_nodes:
                    future = self.multi_helper.run_in_separate_thread(function=self.neo_db.merge_nodes,
                                                                      request_id=request_id,
                                                                      nodes=entries,
                                                                      label=str(key_parts.arguments[0])
                                                                      )
                    all_futures.append(future)

                else:
                    future = self.multi_helper.run_in_separate_thread(function=self.neo_db.merge_edges,
                                                                      request_id=request_id,
                                                                      edges=entries,
                                                                      from_label=key_parts.arguments[1],
                                                                      to_label=key_parts.arguments[2],
                                                                      edge_type=key_parts.arguments[0]
                                                                      )
                all_futures.append(future)

            parallel_results = MultiHelper.wait_on_futures(iterable=all_futures)
            for exception in parallel_results.exceptions:
                self.event_dispatcher.dispatch_event(
                        event=GiraffeEvent(
                                request_id=request_id,
                                event_type=GiraffeEventType.ERROR,
                                message=str(exception),
                                arguments={
                                        'exception': exception,
                                        'message': str(exception)
                                }
                        )
                )
            all_futures.clear()
