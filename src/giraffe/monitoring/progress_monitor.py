import os
import json
import threading
import traceback
from typing import Dict
from typing import Iterable
from giraffe.data_access.abstract.data_and_model_provider import data_and_graph_model
from giraffe.data_access.redis_db import RedisDB
from giraffe.helpers import config_helper
from giraffe.helpers import log_helper
from giraffe.helpers.EventDispatcher import EventDispatcher
from giraffe.helpers.structured_logging_fields import Field
from giraffe.monitoring.giraffe_event import GiraffeEvent
from giraffe.monitoring.giraffe_event import GiraffeEventType
from giraffe.monitoring.ingestion_request import IngestionRequest
from giraffe.helpers.utilities import object_to_json
from giraffe.helpers import utilities


class ProgressMonitor:
    def __init__(self, event_dispatcher: EventDispatcher, config=config_helper.get_config()):
        self.lock = threading.Lock()
        self.config = config
        self.log = log_helper.get_logger(logger_name=__name__)
        self.log.debug('Progress-Monitor started.')
        self.all_tasks: Dict[str, IngestionRequest] = {}
        event_dispatcher.register_callback(callback=self.on_giraffe_event)

    def get_task(self, task_id: str = None):
        if task_id is None:
            return list(self.all_tasks.values())
        return self.all_tasks.get(task_id)

    # 0. Task started and nothing done yet.
    def task_started(self,
                     request_id: str,
                     request_type: str,
                     request_content: str):
        with self.lock:
            new_request = IngestionRequest(ingestion_id=request_id,
                                           ingestion_type=request_type,
                                           request_content=request_content)

            self.all_tasks[request_id] = new_request

    # 1. Reading data from the source, and the graph-model to accompany it.
    def fetching_data_and_models(self,
                                 request_id: str,
                                 source_description):
        with self.lock:
            task = self.get_task(task_id=request_id)
            task.set_status(GiraffeEventType.FETCHING_DATA_AND_MODELS)
            self.log.debug(f'Fetching data-model pairs from source/s described in: {source_description}')

    # 2. Finished reading source_data-model pairs.
    def received_data_and_models(self, request_id: str, data_models: Iterable[data_and_graph_model]):
        with self.lock:
            task = self.get_task(task_id=request_id)
            task.set_status(GiraffeEventType.FINISHED_FETCHING_DATA_AND_MODELS)
            task.map_source_to_model = {item.source_name: item.graph_model for item in data_models}

    # 3. Translating data according to model and writing the results into redis.
    def processing_source_into_redis(self, request_id: str, source_name: str):
        with self.lock:
            task = self.get_task(task_id=request_id)
            task.set_status(GiraffeEventType.WRITING_GRAPH_ELEMENTS_INTO_REDIS)
            self.log.info(f'Processing: {source_name} for request: {request_id}')
            self.log.admin({
                    Field.request_ip: request_id,
                    Field.processing: source_name,
            })

    # 4. Data is ready to be read from redis into neo4j.
    def finished_writing_all_into_redis(self,
                                        request_id: str,
                                        parallel_results):

        with self.lock:
            task = self.get_task(task_id=request_id)
            task.set_status(GiraffeEventType.REDIS_IS_READY_FOR_CONSUMPTION)
            number_of_successful_writes_to_redis = len(parallel_results.results)
            number_of_failed_writes_to_redis = len(parallel_results.exceptions)
            if number_of_failed_writes_to_redis > 0:
                self.log.info(f'Failed redis-writers: {number_of_failed_writes_to_redis}')
                self.log.admin({Field.failed_redis_writers: number_of_failed_writes_to_redis})
            elif number_of_successful_writes_to_redis == 0:
                self.log.warning('Nothing has been written to redis !')  # TODO: Raise an event
            else:
                self.log.info(f'All {number_of_successful_writes_to_redis} redis-writers finished successfully.')
                self.log.admin({
                        Field.request_id: request_id,
                        Field.all_redis_success: number_of_successful_writes_to_redis
                })

    # 5. Writing from redis into neo4j.
    def writing_from_redis_into_neo(self,
                                    result,
                                    redis_db: RedisDB):

        with self.lock:
            source_name = result['source_name']
            processed_keys = result['processed_keys']
            request_id = result['request_id']

            self.log.info(f'Writing to neo4j: {source_name}')
            self.log.admin({
                    Field.writing_to_neo: source_name,
                    Field.request_id: request_id
            })

            redis_sizes = {}
            for key in processed_keys:
                redis_sizes[key] = redis_db.get_cardinality(key=key)
                self.log.debug(f'{key} size in redis = {redis_sizes[key]}')

    # 6. Key has been successfully written from redis into neo4j.
    def key_written_from_redis_into_neo(self,
                                        request_id: str,
                                        key: str):
        with self.lock:
            task = self.get_task(task_id=request_id)
            task.finished_keys.append(key)
            self.log.info(f'Values written from redis to neo4j for: {key}')

    # 7. All sources fully written into neo4j.
    def all_finished(self,
                     request_id: str):
        with self.lock:
            task = self.get_task(task_id=request_id)
            task.set_status(GiraffeEventType.DONE_PROCESSING_REQUEST)
            if set(task.finished_keys) == task.get_redis_keys():
                self.log.info(f'All redis keys successfully written into neo4j : {task.get_redis_keys()}')
            else:
                self.log.error(f'The following expected keys do not seem to have been written: {task.get_redis_keys() - set(task.finished_keys)}')
                # TODO: Raise exception?
            if len(task.errors) > 0:
                self.log.error(f'The following errors were encountered during execution:\n {task.errors}')
            self.dump_to_hard_drive_and_fluent()

    # Intermediate steps

    def processing_redis_content(self, request_id: str, key_prefix: str, redis_db: RedisDB):
        with self.lock:
            keys_found = redis_db.get_key_by_pattern(key_pattern=f'{key_prefix}{self.config.key_separator}*')
            task = self.all_tasks[request_id]
            for key in keys_found:
                cardinality = redis_db.get_cardinality(key=key)
                task.map_redis_key_to_cardinality[key] = cardinality

    def pushing_elements_into_neo4j(self,
                                    request_id: str,
                                    key: str,
                                    how_many: int):
        task = self.get_task(task_id=request_id)
        with self.lock:
            if key not in task.map_redis_key_to_processed_amount:
                task.map_redis_key_to_processed_amount[key] = how_many
            else:
                task.map_redis_key_to_processed_amount[key] += how_many

            elements_type = "nodes" if 'nodes' in key else "edges"
            percent = round(100 * (task.map_redis_key_to_processed_amount[key]) / task.map_redis_key_to_cardinality[key])
            self.log.info(f'{task.map_redis_key_to_processed_amount[key]}/{task.map_redis_key_to_cardinality[key]} ({percent}%) in Neo4j for {key}')
            self.log.admin({
                    Field.request_id: request_id,
                    Field.writing_to_neo: how_many,
                    Field.element_type: elements_type,
                    Field.percent_done: percent
            })

    def deleting_keys_from_redis(self,
                                 request_id: str):
        with self.lock:
            task = self.get_task(task_id=request_id)
            task.set_status(status=GiraffeEventType.DELETING_REDIS_KEYS)

    def merging_into_neo4j(self,
                           request_id: str,
                           element_type: str,
                           element_properties: str,
                           summary):
        with self.lock:
            task = self.get_task(task_id=request_id)
            key = f'{element_type}_{element_properties}'
            counters = utilities.named_tuple_to_dictionary(summary.counters)
            if key not in task.counters:
                task.counters[key] = counters
                return

            for counter in counters:
                if counter not in task.counters[key]:
                    task.counters[key][counter] = counters[counter]
                else:
                    task.counters[key][counter] += counters[counter]

    def error(self,
              request_id: str,
              message: str,
              exception: Exception = None):
        with self.lock:
            task = self.get_task(task_id=request_id)
            task.set_status(GiraffeEventType.ERROR)
            if exception:
                task.errors.append(f'{message}\n{exception}\b{traceback.format_exc()}')
                self.log.error(exception, exc_info=True)
            else:
                task.errors.append(message)
            self.log.error(f'{message} [request_id: {request_id}]')
            # TODO: Decide whether to continue running to halt everything

    # Operational Commands
    def dump_to_hard_drive_and_fluent(self):
        self.log.info(f'Dumping task statuses from memory to folder: {self.config.progress_monitor_dump_folder}')
        for name, task in self.all_tasks.items():
            self.log.admin(json.loads(task.as_json()))  # Passing the info to fluentD if it is listening.
            file_path = os.path.join(self.config.progress_monitor_dump_folder, name)
            with open(file_path, 'w') as dump_file:
                dump_file.write(object_to_json(task, ignored_keys=['log']))

    def dump_and_clear_memory(self):
        self.dump_to_hard_drive_and_fluent()
        self.log.info('Clearing tasks from memory.')
        self.all_tasks.clear()

    def on_giraffe_event(self, event: GiraffeEvent):
        event_type = event.event_type
        if event_type == GiraffeEventType.STARTED_PROCESSING_REQUEST:
            self.task_started(request_id=event.request_id,
                              request_type=event.arguments['request_type'],
                              request_content=event.arguments['request_content'])
        elif event_type == GiraffeEventType.FETCHING_DATA_AND_MODELS:
            self.fetching_data_and_models(request_id=event.request_id,
                                          source_description=event.arguments['source_description'])
        elif event_type == GiraffeEventType.FINISHED_FETCHING_DATA_AND_MODELS:
            self.received_data_and_models(request_id=event.request_id,
                                          data_models=event.arguments['data_models'])
        elif event_type == GiraffeEventType.WRITING_GRAPH_ELEMENTS_INTO_REDIS:
            self.processing_source_into_redis(request_id=event.request_id,
                                              source_name=event.arguments['source_name'])
        elif event_type == GiraffeEventType.REDIS_IS_READY_FOR_CONSUMPTION:
            self.finished_writing_all_into_redis(request_id=event.request_id,
                                                 parallel_results=event.arguments['parallel_results'])
        elif event_type == GiraffeEventType.WRITING_FROM_REDIS_TO_NEO:
            self.writing_from_redis_into_neo(result=event.arguments['details'],
                                             redis_db=event.arguments['redis_db'])
        elif event_type == GiraffeEventType.DELETING_REDIS_KEYS:
            self.deleting_keys_from_redis(request_id=event.request_id)

        elif event_type == GiraffeEventType.DONE_PROCESSING_REQUEST:
            self.all_finished(request_id=event.request_id)
        elif event_type == GiraffeEventType.ERROR:
            self.error(request_id=event.request_id,
                       message=event.arguments['message'],
                       exception=event.arguments['exception'])
        elif event_type == GiraffeEventType.PUSHED_GRAPH_ELEMENTS_INTO_NEO:
            self.merging_into_neo4j(request_id=event.request_id,
                                    element_type=event.arguments['element_type'],
                                    element_properties=event.arguments['element_properties'],
                                    summary=event.arguments['summary'])
        else:
            self.log.debug(f'Ignored event type: {event_type}')
