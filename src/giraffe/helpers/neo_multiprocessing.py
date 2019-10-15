from typing import List, Callable
import multiprocessing as mp

from giraffe.helpers import log_helper
from giraffe.helpers.utilities import list_as_chunks


class MultiProcHelper:
    def __init__(self):
        self.process_pool = mp.Pool()
        self.log = log_helper.get_logger(logger_name=mp.current_process().name)  # Separate log file for each process for simplicity

    def multi_process_list_in_chunks(self,
                                     the_list: List,
                                     batch_function: Callable,
                                     batch_size: int):
        results = self.process_pool.map(func=batch_function, iterable=list_as_chunks(the_list=the_list, chunk_size=batch_size))
        self.log.info(f'Results: {results}')
        return results
