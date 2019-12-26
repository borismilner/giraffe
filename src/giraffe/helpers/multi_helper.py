from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import List

from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper


class MultiHelper:
    def __init__(self, config: ConfigHelper):
        self.config = config
        self.thread_executor = ThreadPoolExecutor(max_workers=config.thread_pool_size)
        self.log = log_helper.get_logger(logger_name='Multi-Helper')
        self.futures: List[Future] = []

    def run_in_parallel_threads(self, function, *args, **kwargs):
        self.thread_executor.submit(function, *args, **kwargs)
