from giraffe.data_access.db_logger import DbLogger
from giraffe.data_access.report_handlers.postgres_handler import PostgresHandler
from giraffe.data_access.report_handlers.sqlite_handler import AdminDbEntry
from giraffe.data_access.report_handlers.sqlite_handler import SqliteHandler


def test_sqlite_reporter():
    current_job_name = 'MyNodes'
    current_job_id = 'Unique-123'

    sqlite_handler = SqliteHandler(db_file_path='meta_info.db')
    sqlite_handler.create_structures_if_not_exists()

    db_logger = DbLogger(handlers=[sqlite_handler])
    db_logger.register_job_name_for_job_id(job_name=current_job_name, job_id=current_job_id)

    for i in range(0, 100):
        db_entry = AdminDbEntry(job_id=current_job_id,
                                job_name=db_logger.get_job_name(current_job_id),
                                category='Reading Table',
                                int_value=i,
                                string_value=f'Finished with {i} rows.')

        db_logger.add_entry(entry=db_entry)


def test_postgres_reporter():
    current_job_name = 'MyNodes'
    current_job_id = '123'
    postgres_handler = PostgresHandler()
    postgres_handler.create_structures_if_not_exists()
    db_logger = DbLogger(handlers=[postgres_handler])
    db_logger.register_job_name_for_job_id(job_name=current_job_name, job_id=current_job_id)

    for i in range(0, 100):
        progress_report = AdminDbEntry(job_id=current_job_id,
                                       job_name=db_logger.get_job_name(current_job_id),
                                       category='Reading Table',
                                       int_value=i,
                                       string_value=f'Finished with {i} rows.')

        db_logger.add_entry(entry=progress_report)
