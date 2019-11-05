from enum import Enum
from typing import List

from giraffe.data_access.formats.progress_report_format import ProgressReport
from giraffe.data_access.formats.status_report_format import StatusReport
from giraffe.data_access.report_handlers.report_handler import ReportHandler
from giraffe.exceptions.logical import UnexpectedOperation
from giraffe.helpers import log_helper


class ProgressLogger:
    class Status(Enum):
        PENDING = 0
        BEGAN = 1
        FINISHED = 2
        ERROR = 666

    def __init__(self, handlers: List[ReportHandler]):
        self.map_job_id_to_name = {}
        self.handlers = handlers
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)

    def register_job_name_for_job_id(self, job_id: str, job_name: str):
        if job_id in self.map_job_id_to_name:
            raise UnexpectedOperation(f'Job-id {job_id} is already assigned to job-name: {self.map_job_id_to_name[job_id]}')
        self.log.debug(f'Job name {job_name} is assigned to job-id: {job_id}.')
        self.map_job_id_to_name[job_id] = job_name

    def unregister_job_id(self, job_id: str):
        self.log.debug(f'Job-id {job_id} is no longer recognized as: {self.map_job_id_to_name[job_id]}')
        self.map_job_id_to_name.pop(job_id, None)

    def status(self, status_report: StatusReport):
        for handler in self.handlers:
            handler.status_report(status_report=status_report)

    def progress(self, progress_report: ProgressReport):
        for handler in self.handlers:
            handler.progress_report(progress_report=progress_report)
