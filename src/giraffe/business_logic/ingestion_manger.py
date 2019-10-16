import os
from typing import List

from giraffe.exceptions.technical import TechnicalError
from giraffe.graph_db.neo_db import NeoDB
from giraffe.helpers.config_helper import ConfigHelper
from giraffe.tools.redis_db import RedisDB
from redis import Redis


class IngestionManager:
    def __init__(self, config_file_path: str = ConfigHelper.default_configurations_file):
        if not os.path.isfile(config_file_path):
            raise TechnicalError(f'Configuration file {config_file_path} does not exist.')
        self.config_helper = ConfigHelper(configurations_ini_file_path=config_file_path)
        self.neo_db: NeoDB = NeoDB(config=self.config_helper)
        self.redis_db: RedisDB = RedisDB(config=self.config_helper)

    def populate_job(self, job_name: str, operation_required: str, operation_arguments: str, items: List):
        r: Redis = self.redis_db.driver
        result = r.sadd(f'{job_name}:{operation_required}:{operation_arguments}', *items)
        assert result == len(items)  # TODO: Handle cases when it's not
