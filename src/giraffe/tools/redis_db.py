import re
import redis
import atexit

from typing import List, Tuple, Iterable

from giraffe.exceptions.logical import MissingJobError
from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from redis import Redis


class RedisDB(object):
    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.log.debug(f'Initialising redis driver.')
        self._driver: Redis = redis.Redis(host=config.redis_host_address, port=config.redis_port)
        self.job_regex = re.compile(config.job_regex)
        atexit.register(self._driver.close)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._driver.close()

    def get_driver(self):
        return self._driver

    def purge_all(self):
        self.log.debug('Purging redis!')
        self._driver.flushall()

    def populate_hashes(self, members: List[Tuple[str, dict]]):
        r: Redis = self._driver
        for key_to_mapping in members:
            r.hmset(name=key_to_mapping[0], mapping=key_to_mapping[1])

    def order_jobs(self, element):
        # Order of the jobs --> <nodes> before <edges> --> Batches sorted by [batch-number] ascending.
        match = self.job_regex.match(element)
        entity_type = match.group(2)
        # noinspection PyRedundantParentheses
        return ('a' if entity_type == 'nodes' else 'z', int(match.group(3)))

    # TODO: COMPLETE THIS ONE
    def pull_job_batches(self, job_name: str):
        r: Redis = self._driver
        all_keys: List[str] = r.keys()
        job_keys = list(filter(lambda key: key.startswith(job_name) + '<', all_keys))  # After `<` comes `nodes`/`edges`
        if len(job_keys) == 0:
            raise MissingJobError(f'No job with the name of {job_name} found.')

        ordered_jobs = sorted(job_keys, key=self.order_jobs, reverse=False)
        self.log.info(f'Discovered {len(ordered_jobs)} batches.')

    def delete_keys(self, keys: Iterable):
        r: Redis = self._driver
        r.delete(*keys)
