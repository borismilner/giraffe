from datetime import datetime
from enum import auto
from enum import Enum
from typing import Union


class GiraffeEventType(Enum):
    RECEIVED_REQUEST = auto()
    STARTED_PROCESSING_REQUEST = auto()
    FETCHING_DATA_AND_MODELS = auto()
    FINISHED_FETCHING_DATA_AND_MODELS = auto()
    WRITING_GRAPH_ELEMENTS_INTO_REDIS = auto()
    REDIS_IS_READY_FOR_CONSUMPTION = auto()
    WRITING_FROM_REDIS_TO_NEO = auto()
    PUSHED_GRAPH_ELEMENTS_INTO_NEO = auto()
    DELETING_REDIS_KEYS = auto()
    DONE_PROCESSING_REQUEST = auto()
    ERROR = auto()
    GENERAL_ERROR = auto()


class GiraffeEvent:
    def __init__(self,
                 request_id: Union[str, None],
                 event_type: GiraffeEventType,
                 message: str,
                 arguments: dict
                 ):
        self.timestamp = datetime.now()
        self.request_id = request_id
        self.event_type = event_type
        self.message = message
        self.arguments = arguments
