import itertools
import os
import time
from typing import Iterable
from typing import List

import psutil
from giraffe.exceptions.logical import NoSuchFileError


def list_as_chunks(the_list: List, chunk_size: int):
    length_of_iterable = len(the_list)
    return (the_list[start_slice:start_slice + chunk_size] for start_slice in range(0,
                                                                                    length_of_iterable,
                                                                                    chunk_size))


def iterable_in_chunks(iterable: Iterable, chunk_size: int):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            return
        yield chunk


def flatten_list(input_list):
    if not input_list:
        return input_list
    if isinstance(input_list[0], list):
        return flatten_list(input_list[0]) + flatten_list(input_list[1:])
    return input_list[:1] + flatten_list(input_list[1:])


def validate_is_file(file_path: str, error_message: str = 'File does not exist.'):
    if not os.path.isfile(file_path):
        raise NoSuchFileError(f'{error_message}: {file_path}')


def get_memory_usage_megabytes() -> float:
    process = psutil.Process(os.getpid())
    used_memory_mb = round(process.memory_info().rss / 1e6, 2)
    return used_memory_mb


class Timer:
    def __init__(self):
        self.__start_time = 0
        self.time_elapsed = 0

    def __enter__(self):
        self.__reset()
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.time_elapsed = time.time() - self.__start_time

    def __reset(self):
        self.__start_time = 0
        self.time_elapsed = 0

    def start(self) -> None:
        self.__start_time = time.time()

    def stop(self) -> float:
        second_elapsed = time.time() - self.__start_time
        self.__reset()
        return second_elapsed
