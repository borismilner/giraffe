import sqlite3

from giraffe.data_access.abstract.db_log_handler import DbLogHandler
from giraffe.data_access.formats.admin_db_entry import AdminDbEntry
from giraffe.helpers import config_helper


class SqliteHandler(DbLogHandler):
    def __init__(self, db_file_path: str, config=config_helper.get_config()):
        self.db_file_path = db_file_path
        self.db = sqlite3.connect(db_file_path)
        self.cursor = self.db.cursor()

    def add_entry(self, entry: AdminDbEntry) -> None:
        query = f'''
        INSERT INTO progress (timestamp, job_name, job_id, category, int_value, string_value)
        VALUES (?, ?, ?, ?, ?, ?)
        '''
        self.cursor.execute(query, [entry.created,
                                    entry.job_name,
                                    entry.job_id,
                                    entry.category,
                                    entry.int_value,
                                    entry.string_value])
        self.db.commit()

    def create_structures_if_not_exists(self) -> bool:
        query = f'''CREATE TABLE IF NOT EXISTS progress (timestamp DATETIME, job_name TEXT, job_id INT, category TEXT, int_value INT, string_value TEXT)'''
        self.cursor.execute(query)
        self.db.commit()
        return True
