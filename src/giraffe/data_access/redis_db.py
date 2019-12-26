import atexit
import threading
from typing import Iterable
from typing import Iterator
from typing import List

import redis
from giraffe.exceptions.logical import MissingKeyError
from giraffe.helpers import config_helper
from giraffe.helpers import log_helper
from giraffe.helpers.dev_spark_helper import DevSparkHelper
from giraffe.helpers.structured_logging_fields import Field
from pyspark.sql import DataFrame
from redis import Redis


class RedisDB(object):
    def __init__(self, config=config_helper.get_config()):
        self.log = log_helper.get_logger(logger_name=f'{self.__class__.__name__}_{threading.current_thread().name}')
        self.log.debug(f'Initialising redis driver.')
        self.driver: Redis = redis.StrictRedis(host=config.redis_host_address, port=config.redis_port, decode_responses=True)
        self.config = config
        self.spark_helper: DevSparkHelper = DevSparkHelper(config=self.config)
        atexit.register(self.driver.close)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.driver.close()

    def get_driver(self):
        return self.driver

    def purge_all(self):
        self.log.debug('Purging redis!')
        self.driver.flushall()

    def delete_keys(self, keys: Iterable):
        r: Redis = self.driver
        self.log.info(f'Deleting keys from redis: {list(keys)}')
        r.delete(*keys)

    def get_cardinality(self, key: str):
        r: Redis = self.driver
        return r.scard(name=key)

    def pull_set_members_in_batches(self, key_pattern: str, batch_size: int) -> Iterator:
        # Shall pull batches of around batch_size from the server and serve them locally through an iterator
        r: Redis = self.driver
        found_keys: List[str] = self.get_key_by_pattern(key_pattern=key_pattern)
        if len(found_keys) != 1:
            raise MissingKeyError(f'No key found with the name of: {key_pattern} (found {len(found_keys)} keys.')
        key = found_keys[0]

        batch_iterator = r.sscan_iter(name=f'{key}', count=batch_size)
        return batch_iterator

    def pull_batch_values_by_keys(self, keys: List):
        batch_values = self.driver.mget(keys=keys)
        return batch_values

    def pull_batch_hashes_by_keys(self, keys: List):
        pipe = self.driver.pipeline()
        for key in keys:
            pipe.hgetall(name=key)
        result = pipe.execute()
        return result

    def get_key_by_pattern(self, key_pattern: str, return_list: bool = True):
        r: Redis = self.driver
        if return_list:
            found_keys: List[str] = [key for key in r.keys(pattern=key_pattern)]
            return found_keys
        else:
            return r.keys(pattern=key_pattern)

    def pull_batch_from_stream(self, stream_name: str, batch_size: int = 1, block_milliseconds: int = None):
        r: Redis = self.driver
        milliseconds_to_block = block_milliseconds if block_milliseconds else int(self.config.redis_stream_milliseconds_block)
        self.log.info(f'Listening on a stream named {stream_name}, pulling in batches of {batch_size} [end of stream within {milliseconds_to_block / 1000} sec.]')
        batch = r.xread(streams={stream_name: 0}, count=batch_size, block=milliseconds_to_block)
        while batch:
            yield [x[1] for x in batch[0][1]]
            bookmark = batch[0][1][-1][0]
            batch = r.xread(streams={stream_name: bookmark}, count=batch_size, block=milliseconds_to_block)
        return

    def write_translator_result_to_redis(self, entry_dict: dict, source_name: str, request_id: str):
        try:
            # Writing entities to redis — each into a set with its own prefix
            for prefix in entry_dict.keys():
                data_frames: List[DataFrame] = entry_dict[prefix]
                self.log.debug(f'Translating and writing to redis: {source_name} [{prefix}] (number of data-frames: {len(data_frames)})')
                for df in data_frames:
                    ready_for_redis = self.spark_helper.get_string_dict_dataframe(df=df, column_name='graph_node')
                    ready_for_redis.write.option('redis_host', self.config.redis_host_address) \
                        .option('redis_column_name', 'graph_node') \
                        .option('redis_set_key', prefix) \
                        .format(source='milner.boris.redis') \
                        .save()
        except Exception as the_exception:
            self.log.error(the_exception, exc_info=True)
            self.log.info(f'Error while translating / writing to redis for {source_name}')
            self.log.admin({Field.request_id: request_id,
                            Field.error: f'Error while translating / writing to redis for {source_name}'
                            })
            raise the_exception

        return {'source_name': source_name, 'processed_keys': list(entry_dict.keys()), 'request_id': request_id}
