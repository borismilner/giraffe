import datetime
from multiprocessing.pool import ThreadPool

from giraffe.business_logic.abstract.data_to_graph_translation_provider import DataToGraphEntitiesProvider
from giraffe.business_logic.abstract.data_to_graph_translation_provider import translation_request
from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.data_access.abstract.data_and_model_provider import DataAndModelProvider
from giraffe.data_access.redis_db import RedisDB
from giraffe.exceptions.giraffe_exception import GiraffeException
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.helpers.multi_helper import MultiHelper
from giraffe.helpers.structured_logging_fields import Field
from giraffe.helpers.utilities import validate_is_file
from giraffe.monitoring.progress_monitor import ProgressMonitor


class Coordinator:
    def __init__(self, config_helper: ConfigHelper,
                 data_and_model_provider: DataAndModelProvider,
                 data_to_graph_translator: DataToGraphEntitiesProvider,
                 progress_monitor: ProgressMonitor):
        self.progress_monitor = progress_monitor
        self.is_ready = False
        self.log = log_helper.get_logger(logger_name=__name__)
        # noinspection PyBroadException
        try:
            self.config = config_helper
            self.redis_db = RedisDB(config=self.config)
            self.data_and_model_provider = data_and_model_provider
            self.thread_pool = ThreadPool()
            self.data_to_graph_translator = data_to_graph_translator
            self.multi_helper = MultiHelper(config=self.config)
            self.im = IngestionManager(config_helper=self.config,
                                       multi_helper=self.multi_helper,
                                       progress_monitor=self.progress_monitor)
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
        self.progress_monitor.task_started(request_id=request_id,
                                           request_type=request_type,
                                           request_content=str(request))

        if request_type == 'white_list':
            file_path = request['file_path']
            source_descriptions = file_path
            validate_is_file(file_path=file_path)
            with open(file_path, 'r') as white_list:
                content = white_list.readlines()
                self.log.admin({Field.white_list_content: content})

        else:
            raise NotImplementedError(f'Currently, only {request_type} request_type supported.')

        # Progress-1: Fetching pairs of data/model for requested data-sources.
        self.progress_monitor.fetching_data_and_models(request_id=request_id,
                                                       source_description=source_descriptions)

        try:
            data_and_models = self.data_and_model_provider.get_data_and_model_for(source_descriptions=source_descriptions)
        except GiraffeException as exception:
            message = f'Failed loading data and models for request: {request_id}'
            self.progress_monitor.error(request_id=request_id,
                                        message=message,
                                        exception=exception)
            raise exception  # Rethrowing

        # Progress-2: Finished fetching pairs of data/model for requested data-sources.
        self.progress_monitor.received_data_and_models(request_id=request_id,
                                                       data_models=data_and_models)

        all_futures = []
        for data_and_model in data_and_models:
            # Progress-3: Writing all graph-elements into redis (concurrently).
            self.progress_monitor.processing_source_into_redis(request_id=request_id,
                                                               source_name=data_and_model.source_name)

            translation_id = f'{request_id}_{data_and_model.source_name}'
            translated_graph_entities = self.data_to_graph_translator.translate(
                    request=translation_request({'src_df': data_and_model.data,
                                                 'model': data_and_model.graph_model,
                                                 'streaming_id': translation_id
                                                 })
            )

            future = self.multi_helper.run_in_separate_thread(function=self.redis_db.write_translator_result_to_redis,
                                                              entry_dict=translated_graph_entities,
                                                              source_name=data_and_model.source_name,
                                                              request_id=request_id,
                                                              )
            all_futures.append(future)

        parallel_results = MultiHelper.wait_on_futures(iterable=all_futures)

        # Progress-4: Graph elements are ready (in redis) to be pushed into neo4j.
        self.progress_monitor.finished_writing_all_into_redis(request_id=request_id,
                                                              future_results=parallel_results)

        for exception in parallel_results.exceptions:
            self.progress_monitor.error(request_id=request_id,
                                        message='Failure on writing to redis.',
                                        exception=exception)
        if not parallel_results.all_ok:
            raise parallel_results.exceptions[0]

        for details in parallel_results.results:
            source_name = details['source_name']
            processed_keys = details['processed_keys']
            request_id: str = details['request_id']

            self.progress_monitor.writing_from_redis_into_neo(result=details,
                                                              redis_db=self.redis_db)

            translation_id = f'{request_id}_{source_name}'
            self.im.process_redis_content(request_id=request_id,
                                          translation_id=translation_id)

            self.redis_db.delete_keys(keys=processed_keys)
            self.log.info(f'{source_name} is ready.')
            self.log.admin({
                    Field.request_id: request_id,
                    Field.ready: source_name
            })
        # Progress-5: Finished writing all redis content into neo4j.
        self.progress_monitor.all_finished(request_id=request_id)
        return request_id

    def _processing_success(self, finished_request_id: str):
        self.log.info(f'Finished processing request: {finished_request_id}')
        self.log.admin({Field.finished_processing_request: finished_request_id})
        self.progress_monitor.dump_to_hard_drive_and_fluent()

    def _processing_error(self, exception: BaseException):
        import traceback
        self.log.error(f'Unhandled exception while handling client request: {exception}')
        self.log.error(''.join(traceback.format_tb(exception.__traceback__)))
        self.progress_monitor.dump_to_hard_drive_and_fluent()

    def begin_processing(self, client_request: dict):
        self.thread_pool.apply_async(self.process_request,
                                     (client_request,),
                                     callback=self._processing_success,
                                     error_callback=self._processing_error)
