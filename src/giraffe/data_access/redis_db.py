import redis
import atexit

from typing import List, Iterable, Iterator

from giraffe.exceptions.logical import MissingKeyError
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from redis import Redis


class RedisDB(object):
    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.log.debug(f'Initialising redis driver.')
        self.driver: Redis = redis.StrictRedis(host=config.redis_host_address, port=config.redis_port, decode_responses=True)
        self.config = config
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
        r.delete(*keys)

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

    def pull_batch_from_stream(self, stream_name: str, batch_size: int = 50_000):
        r: Redis = self.driver
        milliseconds_to_block = int(self.config.redis_stream_milliseconds_block)
        self.log.info(f'Listening on a stream named {stream_name}, pulling in batches pf {batch_size} [end of stream within {milliseconds_to_block / 1000} sec.]')
        batch = r.xread(streams={stream_name: 0}, count=batch_size, block=milliseconds_to_block)
        while batch:
            yield batch
            bookmark = batch[0][1][-1][0]
            batch = r.xread(streams={stream_name: bookmark}, count=batch_size, block=milliseconds_to_block)
