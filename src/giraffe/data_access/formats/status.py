from enum import Enum


class Status(Enum):
    PENDING = 0
    BEGAN = 1
    FINISHED = 2
    ERROR = 666
