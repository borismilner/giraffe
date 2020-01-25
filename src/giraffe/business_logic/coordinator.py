import datetime
from multiprocessing.pool import ThreadPool

from giraffe.business_logic.abstract.data_to_graph_translation_provider import translation_request
from giraffe.business_logic.env_provider import EnvProvider
from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.data_access.redis_db import RedisDB
from giraffe.exceptions.giraffe_exception import GiraffeException
from giraffe.helpers import log_helper
from giraffe.helpers.EventDispatcher import EventDispatcher

from giraffe.helpers.multi_helper import MultiHelper
from giraffe.helpers.structured_logging_fields import Field
from giraffe.helpers.utilities import validate_is_file
from giraffe.monitoring.giraffe_event import GiraffeEvent
from giraffe.monitoring.giraffe_event import GiraffeEventType
from giraffe.monitoring.progress_monitor import ProgressMonitor


class Coordinator:
    def __init__(self,
                 env: EnvProvider,
                 progress_monitor: ProgressMonitor,
                 event_dispatcher: EventDispatcher):
        self.event_dispatcher = event_dispatcher
        self.progress_monitor = progress_monitor
        self.is_ready = False
        self.log = log_helper.get_logger(logger_name=__name__)
        # noinspection PyBroadException
        try:
            self.redis_db = RedisDB(event_dispatcher=self.event_dispatcher, env=env)
            self.data_and_model_provider = env.data_and_model_provider
            self.thread_pool = ThreadPool()
            self.data_to_graph_translator = env.data_to_graph_entities_provider
            self.multi_helper = MultiHelper(config=env.config)
            self.im = IngestionManager(
                    env=env,
                    multi_helper=self.multi_helper,
                    event_dispatcher=self.event_dispatcher
            )
            self.is_ready = True
        except Exception as the_exception:
            self.log.error(the_exception, exc_info=True)
            self.is_ready = False

    def process_request(self, request: dict):

        now = datetime.datetime.now()
        string_timestamp = f'{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}_{now.second}'

        request_type = request['request_type']
        request_id = f"{request['request_id']}_{string_timestamp}"

        self.log.info(f'Progress-Report-ID: {request_id}')

        # Progress-0: Incoming request.
        self.event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.STARTED_PROCESSING_REQUEST,
                        message=f'Starting processing request id: {request_id}',
                        arguments={'request_type': request_type,
                                   'request_content': str(request)
                                   }
                )
        )

        if request_type == 'white_list':
            file_path = request['file_path']
            source_descriptions = file_path
            validate_is_file(file_path=file_path)
            with open(file_path, 'r') as white_list:
                content = white_list.readlines()
                self.log.admin({Field.white_list_content: content})

        else:
            error_message = f'Currently, only {request_type} request_type supported.'
            not_implemented = NotImplementedError(f'Currently, only {request_type} request_type supported.')
            self.event_dispatcher.dispatch_event(
                    event=GiraffeEvent(
                            request_id=request_id,
                            event_type=GiraffeEventType.ERROR,
                            message=error_message,
                            arguments={'message': error_message,
                                       'exception': not_implemented
                                       }
                    )
            )
            raise not_implemented

        # Progress-1: Fetching pairs of data/model for requested data-sources.
        self.event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.FETCHING_DATA_AND_MODELS,
                        message=f'Fetching pairs of data/model for requested data-sources [{request_id}]',
                        arguments={
                                'request_id': request_id,
                                'source_description': source_descriptions
                        },
                )
        )

        try:
            data_and_models = self.data_and_model_provider.get_data_and_model_for(source_descriptions=source_descriptions)
        except GiraffeException as not_implemented:
            message = f'Failed loading data and models for request: {request_id}'
            self.event_dispatcher.dispatch_event(
                    event=GiraffeEvent(
                            request_id=request_id,
                            event_type=GiraffeEventType.ERROR,
                            message=message,
                            arguments={
                                    'request_id': request_id,
                                    'message': message,
                                    'exception': not_implemented
                            },
                    )
            )
            raise not_implemented  # Rethrowing

        # Progress-2: Finished fetching pairs of data/model for requested data-sources.
        self.event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.FINISHED_FETCHING_DATA_AND_MODELS,
                        message=f'Finished fetching pairs of data/model for requested data-sources [{request_id}]',
                        arguments={
                                'request_id': request_id,
                                'data_models': data_and_models
                        }
                )
        )

        all_futures = []
        for data_and_model in data_and_models:
            # Progress-3: Writing all graph-elements into redis (concurrently).
            self.event_dispatcher.dispatch_event(
                    event=GiraffeEvent(
                            request_id=request_id,
                            event_type=GiraffeEventType.WRITING_GRAPH_ELEMENTS_INTO_REDIS,
                            message=f'Writing graph-element into redis (concurrently) [{data_and_model.source_name}]',
                            arguments={
                                    'request_id': request_id,
                                    'source_name': data_and_model.source_name
                            }
                    )
            )

            translation_id = f'{request_id}_{data_and_model.source_name}'
            translated_graph_entities = self.data_to_graph_translator.translate(
                    request=translation_request({'src_df': data_and_model.data,
                                                 'model': data_and_model.graph_model,
                                                 'streaming_id': translation_id
                                                 })
            )

            future = self.multi_helper.run_in_separate_thread(
                    function=self.redis_db.write_translator_result_to_redis,
                    entry_dict=translated_graph_entities,
                    source_name=data_and_model.source_name,
                    request_id=request_id,
            )
            all_futures.append(future)

        parallel_results = MultiHelper.wait_on_futures(iterable=all_futures)

        # Progress-4: Graph elements are ready (in redis) to be pushed into neo4j.
        self.event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.REDIS_IS_READY_FOR_CONSUMPTION,
                        message=f'Graph elements are ready (in redis) to be pushed into neo4j [{request_id}]',
                        arguments={
                                'request_id': request_id,
                                'parallel_results': parallel_results
                        }
                )
        )

        for not_implemented in parallel_results.exceptions:
            self.event_dispatcher.dispatch_event(
                    event=GiraffeEvent(
                            request_id=request_id,
                            event_type=GiraffeEventType.ERROR,
                            message='Failure on writing to redis.',
                            arguments={
                                    'request_id': request_id,
                                    'message': 'Failure on writing to redis.',
                                    'exception': not_implemented
                            }
                    )
            )

        if not parallel_results.all_ok:
            raise parallel_results.exceptions[0]

        for details in parallel_results.results:
            source_name = details['source_name']
            processed_keys = details['processed_keys']
            request_id: str = details['request_id']

            self.event_dispatcher.dispatch_event(
                    event=GiraffeEvent(
                            request_id=request_id,
                            event_type=GiraffeEventType.WRITING_FROM_REDIS_TO_NEO,
                            message=f'Graph elements are ready (in redis) to be pushed into neo4j [{request_id}]',
                            arguments={
                                    'request_id': request_id,
                                    'source_name': source_name,
                                    'processed_keys': processed_keys,
                                    'details': details,
                                    'redis_db': self.redis_db
                            }
                    )
            )

            translation_id = f'{request_id}_{source_name}'
            self.im.process_redis_content(request_id=request_id,
                                          translation_id=translation_id)

            self.event_dispatcher.dispatch_event(
                    event=GiraffeEvent(
                            request_id=request_id,
                            event_type=GiraffeEventType.DELETING_REDIS_KEYS,
                            message=f'Deleting done-with keys from redis [{processed_keys}]',
                            arguments={
                                    'request_id': request_id,
                                    'keys': processed_keys
                            }
                    )
            )
            self.redis_db.delete_keys(keys=processed_keys)
            self.log.info(f'{source_name} is ready.')
            self.log.admin(
                    {
                            Field.request_id: request_id,
                            Field.ready: source_name
                    }
            )

        # Progress-5: Finished writing all redis content into neo4j.
        self.event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.DONE_PROCESSING_REQUEST,
                        message=f'Done processing request: {request_id}',
                        arguments={
                                'request_id': request_id
                        }
                )
        )
        return request_id

    def processing_success_callback(self, finished_request_id: str):
        self.log.info(f'Finished processing request: {finished_request_id}')
        self.log.admin({Field.finished_processing_request: finished_request_id})
        self.progress_monitor.dump_to_hard_drive_and_fluent()

    def processing_error_callback(self, exception: BaseException):
        import traceback
        self.log.error(f'Unhandled exception while handling client request: {exception}')
        self.log.error(''.join(traceback.format_tb(exception.__traceback__)))
        self.progress_monitor.dump_to_hard_drive_and_fluent()
