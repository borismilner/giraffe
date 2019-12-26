import datetime
from multiprocessing.pool import ThreadPool

from giraffe.business_logic.abstract.data_to_graph_translation_provider import DataToGraphEntitiesProvider
from giraffe.business_logic.abstract.data_to_graph_translation_provider import translation_request
from giraffe.business_logic.ingestion_manger import IngestionManager
from giraffe.data_access.abstract.data_and_model_provider import DataAndModelProvider
from giraffe.data_access.redis_db import RedisDB
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.helpers.structured_logging_fields import Field
from giraffe.helpers.utilities import validate_is_file


class Coordinator:
    def __init__(self, config_helper: ConfigHelper,
                 data_and_model_provider: DataAndModelProvider,
                 data_to_graph_translator: DataToGraphEntitiesProvider):
        self.is_ready = False
        self.log = log_helper.get_logger(logger_name=__name__)
        # noinspection PyBroadException
        try:
            self.config = config_helper
            self.redis_db = RedisDB(config=self.config)
            self.data_and_model_provider = data_and_model_provider
            self.thread_pool = ThreadPool()
            self.data_to_graph_translator = data_to_graph_translator
            self.is_ready = True
        except Exception as the_exception:
            self.log.error(the_exception, exc_info=True)
            self.is_ready = False

    # Called for every success on writing a key to redis
    def _on_success_writing_to_redis(self, results: dict) -> None:
        im = IngestionManager(config_helper=self.config)
        source_name = results['source_name']
        processed_keys = results['processed_keys']
        request_id = results['request_id']
        if not im.is_ready:
            self.log.error(f'Failed initializing an ingestion-manager component for {source_name} - aborting.')
            self.log.error(f'Keys not handled: {processed_keys}.')
            self.log.admin({
                    Field.request_id: request_id,
                    Field.error: f'Failed starting ingestion manager for: {processed_keys}'
            })
            return  # TODO: Check whether it is not too much of an "error swallowing".
        self.log.info(f'Writing to neo4j: {source_name}')
        self.log.admin({
                Field.writing_to_neo: source_name,
                Field.request_id: request_id
        })

        redis_sizes = {}
        for key in processed_keys:
            redis_sizes[key] = self.redis_db.get_cardinality(key=key)
            self.log.debug(f'{key} size in redis = {redis_sizes[key]}')

        self.log.admin({
                Field.request_id: request_id,
                Field.redis_sizes: redis_sizes
        })

        for key in processed_keys:
            key_parts = key.split(':')
            prefix = ':'.join((key_parts[0], key_parts[1]))
            im.process_redis_content(key_prefix=prefix, request_id=request_id)
            self.log.info(f'Values written from redis to neo4j for: {prefix}')
        self.redis_db.delete_keys(keys=processed_keys)
        self.log.info(f'{source_name} is ready.')
        self.log.admin({
                Field.request_id: request_id,
                Field.ready: source_name
        })

    def _on_error_before_writing_to_redis(self, exception: BaseException) -> None:
        import traceback
        self.log.error(f'Unhandled exception before writing to redis: {exception}')
        error_message = ''.join(traceback.format_tb(exception.__traceback__))
        self.log.error(error_message)
        self.log.admin({Field.error: error_message})

    def process_request(self, request: dict):

        t = datetime.datetime.now()

        request_type = request['request_type']
        request_id = f"{request['request_id']}_{t.year}_{t.month}_{t.day}_{t.hour}_{t.minute}_{t.second}"

        self.log.info(f'Processing request-id: {request_id} ({request_type}) {request}')
        self.log.admin({
                Field.request_id: request_id,
                Field.request_type: request_type,
                Field.request: request
        })

        if request_type == 'white_list':
            file_path = request['file_path']
            source_descriptions = file_path
            validate_is_file(file_path=file_path)
            with open(file_path, 'r') as white_list:
                content = white_list.readlines()
                self.log.admin({Field.white_list_content: content})

        else:
            raise NotImplementedError(f'Currently, only {request_type} request_type supported.')

        data_and_models = self.data_and_model_provider.get_data_and_model_for(source_descriptions=source_descriptions)

        all_writer_threads = []
        for data_and_model in data_and_models:
            translated_graph_entities = self.data_to_graph_translator.translate(
                    request=translation_request({'src_df': data_and_model.data,
                                                 'model': data_and_model.graph_model,
                                                 'streaming_id': f'{request_id}_{data_and_model.source_name}'
                                                 })
            )
            self.log.info(f'Processing: {data_and_model.source_name}')
            self.log.admin({
                    Field.request_ip: request_id,
                    Field.processing: data_and_model.source_name,
            })
            redis_writer = self.thread_pool.apply_async(
                    self.redis_db.write_translator_result_to_redis,
                    kwds={'entry_dict': translated_graph_entities,
                          'source_name': data_and_model.source_name,
                          'request_id': request_id
                          },
                    callback=self._on_success_writing_to_redis,
                    error_callback=self._on_error_before_writing_to_redis
            )

            all_writer_threads.append(redis_writer)
        unsuccessful_writers = 0
        for writer in all_writer_threads:
            writer.wait()
            if not writer.successful():
                unsuccessful_writers += 1
        if unsuccessful_writers > 0:
            self.log.info(f'Failed redis-writers: {unsuccessful_writers}')
            self.log.admin({Field.failed_redis_writers: unsuccessful_writers})
        else:
            self.log.info(f'All {len(all_writer_threads)} redis-writers finished successfully.')
            self.log.admin({
                    Field.request_id: request_id,
                    Field.all_redis_success: len(all_writer_threads)
            })

        return request_id

    def _processing_success(self, finished_request_id: str):
        self.log.info(f'Finished processing request: {finished_request_id}')
        self.log.admin({Field.finished_processing_request: finished_request_id})

    def _processing_error(self, exception: BaseException):
        import traceback
        self.log.error(f'Unhandled exception while handling client request: {exception}')
        self.log.error(''.join(traceback.format_tb(exception.__traceback__)))

    def begin_processing(self, client_request: dict):
        self.thread_pool.apply_async(self.process_request,
                                     (client_request,),
                                     callback=self._processing_success,
                                     error_callback=self._processing_error)
