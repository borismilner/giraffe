import psycopg2
from giraffe.data_access.formats.admin_db_entry import AdminDbEntry
from giraffe.data_access.abstract.db_log_handler import DbLogHandler
from giraffe.helpers import config_helper
from giraffe.helpers.config_helper import ConfigHelper
from psycopg2 import sql


class PostgresHandler(DbLogHandler):
    def __init__(self, host: str = "localhost", database: str = "giraffe", username: str = "postgres", password: str = "098098", config: ConfigHelper = ConfigHelper()):
        self.config = config_helper.ConfigHelper()
        self.connection = psycopg2.connect(
                host=host,
                database=database,
                user=username,
                password=password
        )

        self.cursor = self.connection.cursor()

    def add_entry(self, entry: AdminDbEntry) -> None:
        query = sql.SQL("INSERT INTO {} (timestamp, job_name, job_id, category, int_value, string_value) VALUES (%s, %s, %s, %s, %s, %s)").format(sql.Identifier(self.config.admin_db_table_name))
        self.cursor.execute(query, [entry.created,
                                    entry.job_name,
                                    entry.job_id,
                                    entry.category,
                                    entry.int_value,
                                    entry.string_value])
        self.connection.commit()

    def create_structures_if_not_exists(self) -> bool:
        query = f'''CREATE TABLE IF NOT EXISTS {self.config.admin_db_table_name} (timestamp timestamp, job_name TEXT, job_id INT, category TEXT, int_value INT, string_value TEXT)'''
        self.cursor.execute(query)
        self.connection.commit()
        return True
