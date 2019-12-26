from datetime import datetime


class AdminDbEntry:
    def __init__(self,
                 job_id: str,
                 job_name: str,
                 category: str,
                 int_value: int = None,
                 string_value: str = None):
        self.created = datetime.now()
        self.job_id = job_id
        self.job_name = job_name
        self.category = category
        self.int_value = int_value
        self.string_value = string_value
