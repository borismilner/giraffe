import os

from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db.neo_db import NeoDB
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.tools.redis_db import RedisDB


class IngestionManager:
    def __init__(self, config_file_path: str):
        if not os.path.isfile(config_file_path):
            raise TechnicalError(f'Configuration file {config_file_path} does not exist.')
        self.config_helper = ConfigHelper(configurations_ini_file_path=config_file_path)
        self.neo_db: NeoDB = NeoDB(config=self.config_helper)
        self.redis_db: RedisDB = RedisDB(config=self.config_helper)
