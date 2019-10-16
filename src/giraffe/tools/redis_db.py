import re
import redis
import atexit

from typing import List, Tuple, Iterable, Iterator

from giraffe.exceptions.logical import MissingKeyError
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from redis import Redis


class RedisDB(object):
    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.log.debug(f'Initialising redis driver.')
        self.driver: Redis = redis.Redis(host=config.redis_host_address, port=config.redis_port)
        self.job_regex = re.compile(config.job_regex)
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

    def order_jobs(self, element):
        # Order of the jobs --> <nodes> before <edges> --> Batches sorted by [batch-number] ascending.
        match = self.job_regex.match(element)
        entity_type = match.group(2)
        # noinspection PyRedundantParentheses
        return ('a' if entity_type == 'nodes' else 'z', int(match.group(3)))

    def delete_keys(self, keys: Iterable):
        r: Redis = self.driver
        r.delete(*keys)

    def pull_in_batches(self, key: str, batch_size: int) -> Iterator:
        # Shall pull batches of around batch_size from the server and serve them locally through an iterator
        r: Redis = self.driver
        found_keys: List[str] = r.keys(pattern=key)
        if len(found_keys) != 1:
            raise MissingKeyError(f'No key found with the name of: {key} (found {len(found_keys)} keys.')

        batch_iterator = r.sscan_iter(key, match=None, count=batch_size)
        return batch_iterator
