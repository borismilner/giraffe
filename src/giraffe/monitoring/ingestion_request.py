from enum import Enum
from datetime import datetime
from typing import Dict
from typing import Set

from giraffe.helpers import log_helper
from giraffe.helpers.structured_logging_fields import Field
from giraffe.helpers.utilities import object_to_json
from giraffe.helpers.utilities import timestamp_to_str


class RequestStatus(Enum):
    STARTED = 0
    STARTED_READING_DATA_AND_MODELS = 1
    FINISHED_READING_DATA_AND_MODELS = 2
    TRANSLATING_INTO_REDIS = 3
    READY_TO_WRITE_FROM_REDIS_INTO_NEO = 4
    STARTED_WRITING_FROM_REDIS_TO_NEO = 5
    FINISHED_WRITING_FROM_REDIS_TO_NEO = 6
    DONE = 7
    ERROR = 8


class IngestionRequest:
    def __init__(self, ingestion_id: str, ingestion_type: str, request_content: str):
        self.status = RequestStatus.STARTED
        self.map_status_to_timestamp: Dict[str, str] = {}
        self.id = ingestion_id

        self.log = log_helper.get_logger(logger_name=__name__)

        self.request_body = request_content
        self.start_timestamp = datetime.now()
        self.start_time_unix = self.now_as_unix_timestamp()
        self.end_timestamp = None
        self.end_time_unix = None
        self.map_source_to_model = {}
        self.map_redis_key_to_cardinality = {}
        self.map_redis_key_to_processed_amount = {}
        self.counters = {}

        self.finished_keys = []
        self.errors = []

        self.set_status(status=RequestStatus.STARTED)
        self.log.info(f'Processing request-id: {ingestion_id} ({ingestion_type}) {request_content}')
        self.log.admin({
                Field.request_id: ingestion_id,
                Field.request_type: ingestion_type,
                Field.request: request_content
        })

    @staticmethod
    def now_as_unix_timestamp() -> int:
        return int(datetime.now().timestamp())

    def get_redis_keys(self) -> Set[str]:
        return set(self.map_redis_key_to_cardinality.keys())

    def set_status(self, status: RequestStatus):
        self.map_status_to_timestamp[str(status)] = timestamp_to_str(timestamp=datetime.now())
        if status == RequestStatus.STARTED:
            self.start_time_unix = self.now_as_unix_timestamp()
        elif status == RequestStatus.DONE:
            self.end_timestamp = datetime.now()
            self.end_time_unix = self.now_as_unix_timestamp()
        self.status = status

    def as_json(self) -> str:
        return object_to_json(o=self, ignored_keys=['log'])
