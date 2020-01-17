from typing import List

from giraffe.data_access.abstract.db_log_handler import DbLogHandler
from giraffe.data_access.formats.admin_db_entry import AdminDbEntry
from giraffe.exceptions.logical import UnexpectedOperation
from giraffe.helpers import log_helper


class DbLogger:

    def __init__(self, handlers: List[DbLogHandler]):
        self.map_job_id_to_name = {}
        self.handlers = handlers
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)

    def get_job_name(self, job_id: str):
        return self.map_job_id_to_name[job_id]

    def register_job_name_for_job_id(self, job_id: str, job_name: str):
        if job_id in self.map_job_id_to_name:
            raise UnexpectedOperation(f'Job-id {job_id} is already assigned to job-name: {self.map_job_id_to_name[job_id]}')
        self.log.debug(f'Job name {job_name} is assigned to job-id: {job_id}.')
        self.map_job_id_to_name[job_id] = job_name

    def unregister_job_id(self, job_id: str):
        self.log.debug(f'Job-id {job_id} is no longer recognized as: {self.map_job_id_to_name[job_id]}')
        self.map_job_id_to_name.pop(job_id, None)

    def add_entry(self, entry: AdminDbEntry):
        for handler in self.handlers:
            handler.add_entry(entry=entry)
