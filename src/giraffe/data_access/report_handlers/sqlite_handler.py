import sqlite3

from giraffe.data_access.formats.progress_report_format import ProgressReport
from giraffe.data_access.formats.status_report_format import StatusReport
from giraffe.data_access.report_handlers.report_handler import ReportHandler


class SqliteHandler(ReportHandler):
    def __init__(self, db_file_path: str):
        self.db_file_path = db_file_path
        self.db = sqlite3.connect(db_file_path)
        self.cursor = self.db.cursor()

    def status_report(self, status_report: StatusReport) -> None:
        pass

    def progress_report(self, progress_report: ProgressReport) -> None:
        query = f'''
        INSERT INTO progress (timestamp, job_name, job_id, stage, details, status)
        VALUES (?, ?, ?, ?, ?, ?)
        '''
        self.cursor.execute(query, [progress_report.created,
                                    progress_report.job_name,
                                    progress_report.job_id,
                                    progress_report.stage,
                                    progress_report.details,
                                    progress_report.status])
        self.db.commit()

    def create_structures_if_not_exists(self) -> bool:
        query = f'''CREATE TABLE progress (timestamp DATETIME, job_name TEXT, job_id INT, stage TEXT, details TEXT, status TEXT)'''
        self.cursor.execute(query)
        self.db.commit()
        return True
