from giraffe.data_access.formats.progress_report_format import ProgressReport
from giraffe.data_access.report_handlers.sqlite_handler import SqliteHandler
from giraffe.data_access.formats.status import Status
from giraffe.data_access.reporter import ProgressLogger


def test_sqlite_reporter():
    current_job_name = 'MyNodes'
    current_job_id = 'Unique-123'

    sqlite_handler = SqliteHandler(db_file_path='meta_info.db')
    sqlite_handler.create_structures_if_not_exists()

    p_logger = ProgressLogger(handlers=[sqlite_handler])
    p_logger.register_job_name_for_job_id(job_name=current_job_name, job_id=current_job_id)

    for i in range(0, 100):
        progress_report = ProgressReport(job_id=current_job_id,
                                         job_name=p_logger.get_job_name(current_job_id),
                                         stage='Reading Table',
                                         details=f'Collecting {i} rows',
                                         status=Status.FINISHED)

        p_logger.progress(progress_report=progress_report)
