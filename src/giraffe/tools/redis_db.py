import redis
import atexit

from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper
from redis import Redis


class RedisDB(object):
    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.log.debug(f'Initialising redis driver.')
        self._driver: Redis = redis.Redis(host=config.redis_host_address, port=config.redis_port)
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
