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

    def get_key_by_pattern(self, key_pattern: str):
        r: Redis = self.driver
        found_keys: List[str] = [key for key in r.keys(pattern=key_pattern)]
        return found_keys
