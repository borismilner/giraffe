import collections
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from typing import Iterable
from typing import List

from giraffe.helpers import log_helper
from giraffe.helpers.config_helper import ConfigHelper

WaitResults = collections.namedtuple('WaitResults', ['results', 'exceptions', 'total', 'succeeded', 'failed', 'all_ok'])


class MultiHelper:
    def __init__(self, config: ConfigHelper):
        self.config = config
        self.thread_executor = ThreadPoolExecutor(max_workers=config.thread_pool_size)
        self.log = log_helper.get_logger(logger_name='Multi-Helper')
        self.futures: List[Future] = []

    def run_in_separate_thread(self, function, *args, **kwargs):
        future = self.thread_executor.submit(function, *args, **kwargs)
        return future

    @staticmethod
    def wait_on_futures(iterable: Iterable) -> WaitResults:
        results = []
        exceptions = []
        total = 0
        succeeded = 0
        failed = 0
        all_ok = True

        completed_futures = wait(fs=iterable, return_when='ALL_COMPLETED')
        for result in completed_futures.done:
            total += 1
            try:
                results.append(result)
                succeeded += 1
            except Exception as exception:
                all_ok = False
                failed += 1
                exceptions.append(exception)
        return WaitResults(results=results,
                           exceptions=exceptions,
                           total=total,
                           succeeded=succeeded,
                           failed=failed,
                           all_ok=all_ok)
