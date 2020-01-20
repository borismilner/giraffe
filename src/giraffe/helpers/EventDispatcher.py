import atexit
import pickle
import collections
from enum import Enum, auto
from typing import Callable
from typing import List

from giraffe.helpers import log_helper
from giraffe.helpers.communicator import Communicator

GiraffeEvent = collections.namedtuple('GiraffeEvent', ['request_id',
                                                       'event_type',
                                                       'message',
                                                       'arguments'])


class GiraffeEventType(Enum):
    STARTED = auto()
    FETCHING_DATA_AND_MODELS = auto()
    FINISHED_FETCHING_DATA_AND_MODELS = auto()
    WRITING_GRAPH_ELEMENTS_INTO_REDIS = auto()
    REDIS_IS_READY_FOR_CONSUMPTION = auto()
    WRITING_FROM_REDIS_TO_NEO = auto()
    PUSHED_GRAPH_ELEMENTS_INTO_NEO = auto()
    DELETING_REDIS_KEYS = auto()
    DONE_PROCESSING_REQUEST = auto()
    ERROR = auto()
    GENERAL_EVENT = auto()
    GENERAL_ERROR = auto()


class EventDispatcher:
    def __init__(self, tcp_enabled: bool = True, tcp_host: str = 'localhost', tcp_port: int = 65432):
        self.listeners: List[Callable] = []
        self.log = log_helper.get_logger(logger_name='Event-Dispatcher')
        self.tcp_server: Communicator = Communicator(host=tcp_host, port=tcp_port)
        self.tcp_enabled = tcp_enabled
        if self.tcp_enabled:
            self.tcp_server.start_server()
            atexit.register(self.tcp_server.server_socket.close)

    def dispatch_event(self, event: GiraffeEvent):
        self.log.debug(event.message)
        for callback in self.listeners:
            try:
                callback(event)
            except Exception as exception:  # Assuming it's the fault of callback-author
                self.log.warning(f'Failed calling a callback function: {exception}')

        if self.tcp_enabled and len(self.tcp_server.listeners) > 0:
            try:
                self.tcp_server.broadcast_to_clients(data=pickle.dumps(event).hex())
            except Exception as exception:  # Some events are non-pickle-friendly
                self.log.debug(f'Probably failed to pickle:\n{exception}')

    def register_callback(self, callback: Callable):
        self.listeners.append(callback)
