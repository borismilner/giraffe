from logging import Logger
from time import sleep
from typing import Callable


def wait_for(condition: Callable[[], bool],
             condition_name: str,
             sec_sleep: int,
             retries: int = 1,
             logger: Logger = None) -> bool:
    if condition():
        if logger:
            logger.info(f'[{condition_name}] — OK :)')
        return True

    for retries_left in range(retries, 0, -1):
        if logger:
            logger.info(f'[{condition_name}] Sleeping {sec_sleep} seconds [{retries_left} retries left.]')
        sleep(sec_sleep)
        if condition():
            if logger:
                logger.info(f'[{condition_name}] — OK :)')
            return True
    return False
