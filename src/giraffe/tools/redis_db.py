import redis
import atexit

from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper


class RedisDB(object):
    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.log.debug(f'Initialising redis driver.')
        self._driver = redis.Redis(host=config.redis_host_address, port=config.redis_port)
        atexit.register(self.__exit__, None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            _ = self._driver.client_list()
            self._driver.close()
        except redis.ConnectionError:
            pass

    def get_driver(self):
        return self._driver
