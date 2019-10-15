import os
import configparser

from giraffe.exceptions.technical import TechnicalError
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

        self.neo_host_address = self.config['NEO4J']['HOST']
        self.neo_username = self.config['NEO4J']['USERNAME']
        self.neo_password = self.config['NEO4J']['PASSWORD']
        self.neo_bolt_port = self.config['NEO4J']['BOLT_PORT']

        self.neo_bolt_uri = f'bolt://{self.neo_host_address}:{self.neo_bolt_port}'

        # Reading REDIS connection details from configuration-file
        self.redis_host_address = self.config['REDIS']['HOST']
        self.redis_username = self.config['REDIS']['USERNAME']
        self.redis_password = self.config['REDIS']['PASSWORD']
        self.redis_port = self.config['REDIS']['PORT']

        # Unit-Testing settings

        self.test_label = self.config['TESTING']['test_label']
        self.test_edge_type = self.config['TESTING']['test_edge_type']
        self.test_property = self.config['TESTING']['test_property']
        self.number_of_test_nodes = int(self.config['TESTING']['number_of_test_nodes'])
        self.number_of_test_edges = int(self.config['TESTING']['number_of_test_edges'])
