import os
import configparser

from giraffe.exceptions.technical_error import TechnicalError
from giraffe.helpers import log_helper


class ConfigHelper:
    __module_folder = os.path.dirname(os.path.abspath(__file__))
    __default_config_relative_to_module = '../configuration/defaults.ini'
    __default_configurations_file = os.path.join(__module_folder, __default_config_relative_to_module)

    def __init__(self, configurations_ini_file_path: str = __default_configurations_file):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        if not os.path.isfile(configurations_ini_file_path):
            raise TechnicalError(f'{configurations_ini_file_path} does not exist.')

        config_file_path = os.path.abspath(configurations_ini_file_path)
        self.log.info(f'Configuration file: {config_file_path}')
        self.config = configparser.ConfigParser()
        self.config.read(configurations_ini_file_path)

        # Reading Neo4j connection details from configuration-file

        self.host_address = self.config['NEO4J']['HOST']
        self.username = self.config['NEO4J']['USERNAME']
        self.password = self.config['NEO4J']['PASSWORD']
        self.bolt_port = self.config['NEO4J']['BOLT_PORT']

        self.bolt_uri = f'bolt://{self.host_address}:{self.bolt_port}'
