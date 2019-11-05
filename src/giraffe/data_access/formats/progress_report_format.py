from datetime import datetime

from giraffe.data_access.formats.status import Status


class ProgressReport:
    def __init__(self,
                 job_id: str,
                 job_name: str,
                 stage: str,
                 details: str,
                 status: Status):
        self.created = datetime.now()
        self.job_name = job_name
        self.job_id = job_id
        self.stage = stage
        self.details = details
        self.status = status.name
