import time
from typing import List

from giraffe.helpers import log_helper
from giraffe.helpers.utilities import Timer
from giraffe.helpers.neo_multiprocessing import MultiProcHelper

import multiprocessing as mp

sleep_time_simulation_sec = 2.25


def processing_function(batch: List, time_to_sleep: float = sleep_time_simulation_sec):
    log = log_helper.get_logger(logger_name=mp.current_process().name)  # Separate log file for each process for simplicity
    log.debug(f'Sleeping for {sleep_time_simulation_sec}')
    time.sleep(time_to_sleep)  # Simulate some work
    return len(batch)


def test_neo_multi_proc_helper():
    timer = Timer()
    test_number_of_items = 1_000_050
    test_batch_size = 50_000
    list_to_process = [i for i in range(0, test_number_of_items)]

    number_of_whole_batches = test_number_of_items // test_batch_size
    last_batch_size = test_number_of_items % test_batch_size
    expected_number_of_batches = number_of_whole_batches + (1 if last_batch_size != 0 else 0)
    expected_time_without_multi_processing = expected_number_of_batches * sleep_time_simulation_sec

    mph = MultiProcHelper()
    with timer:
        results = mph.multi_process_list_in_chunks(the_list=list_to_process, batch_function=processing_function, batch_size=test_batch_size)

    assert len(results) == expected_number_of_batches
    assert all(item == results[0] == test_batch_size for item in results[0:-1])
    assert results[-1] == last_batch_size
    assert timer.time_elapsed < expected_time_without_multi_processing