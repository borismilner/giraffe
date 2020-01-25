import atexit
import os
import sys
from logging.handlers import RotatingFileHandler

from giraffe.business_logic.abstract.data_to_graph_translation_provider import DataToGraphEntitiesProvider
from giraffe.business_logic.data_to_entities_translators.mock_translator import MockDataToGraphEntitiesProvider
from giraffe.data_access.abstract.data_and_model_provider import DataAndModelProvider
from giraffe.data_access.data_model_providers.mock_data_model_provider import MockDataAndModelProvider
from giraffe.helpers import config_helper
from giraffe.helpers import log_helper
from giraffe.helpers.dev_spark_helper import DevSparkHelper
from giraffe.helpers.utilities import validate_is_file


class EnvProvider:
    def __init__(self, cmd_line_args=None):
        self.log = log_helper.get_logger(logger_name=__name__)
        configuration_ini_file_path = None if cmd_line_args is None else cmd_line_args[0].config_ini
        if configuration_ini_file_path is None:
            self.config = config_helper.get_config()
        else:
            config_ini_file = configuration_ini_file_path
            validate_is_file(file_path=config_ini_file)
            self.config = config_helper.get_config(configurations_ini_file_path=config_ini_file)

        # -------

        self.logging_file_path = os.path.join(self.config.logs_storage_folder, 'giraffe.log')
        file_handler = RotatingFileHandler(filename=self.logging_file_path,
                                           mode='a',
                                           maxBytes=1_000_000,
                                           backupCount=3,
                                           encoding='utf-8',
                                           delay=False)
        file_handler.setFormatter(log_helper.log_row_format)
        log_helper.add_handler(handler=file_handler)

        atexit.register(log_helper.stop_listener)

        # -------

        self.execution_env = self.config.execution_environment

        # -------

        if self.execution_env == 'dev':
            self.data_and_model_provider: DataAndModelProvider = MockDataAndModelProvider()
            self.data_to_graph_entities_provider: DataToGraphEntitiesProvider = MockDataToGraphEntitiesProvider()
            self.spark_helper = DevSparkHelper(config=self.config)
        elif self.execution_env == 'cortex':
            raise NotImplementedError('Implemented in cortex')
        else:
            self.log.info(f'Unexpected value in configuration file for execution_environment: {self.execution_env}')
            sys.exit(1)
