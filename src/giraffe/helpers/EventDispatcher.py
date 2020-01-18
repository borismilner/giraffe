import collections
from enum import Enum
from typing import Callable
from typing import List

from giraffe.helpers import log_helper

GiraffeEvent = collections.namedtuple('GiraffeEvent', ['request_id',
                                                       'event_type',
                                                       'message',
                                                       'arguments'])


class GiraffeEventType(Enum):
    STARTED = 0
    FETCHING_DATA_AND_MODELS = 1
    FINISHED_FETCHING_DATA_AND_MODELS = 2
    WRITING_GRAPH_ELEMENTS_INTO_REDIS = 3
    REDIS_IS_READY_FOR_CONSUMPTION = 4
    WRITING_FROM_REDIS_TO_NEO = 5
    DELETING_REDIS_KEYS = 6
    DONE_PROCESSING_REQUEST = 7
    ERROR = 8


class EventDispatcher:
    def __init__(self):
        self.listeners: List[Callable] = []
        self.log = log_helper.get_logger(logger_name='Event-Dispatcher')

    def dispatch_event(self, event: GiraffeEvent):
        self.log.debug(event.message)
        for callback in self.listeners:
            try:
                callback(event)
            except Exception as exception:  # Assuming it's the fault of callback-author
                self.log.warning(f'Failed calling a callback function: {exception}')

    def register_callback(self, callback: Callable):
        self.listeners.append(callback)
