import os
import configparser
from neobolt.exceptions import ServiceUnavailable

from giraffe.exceptions.technical_error import TechnicalError
from giraffe.helpers import log_helper
from py2neo import Graph


class NeoDB:
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
        self.graph = Graph(uri=self.config['NEO4J']['HOST'],
                           user=self.config['NEO4J']['USERNAME'],
                           password=self.config['NEO4J']['PASSWORD'])
        try:
            db_kernel_start = self.graph.database.kernel_start_time
        except ServiceUnavailable as _:
            raise TechnicalError(f'Neo4j does not seem to be active at {self.config["NEO4J"]["HOST"]}')
        self.log.debug(f'Neo4j is active since {db_kernel_start}.')
