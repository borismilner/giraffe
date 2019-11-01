import os
import configparser

from giraffe.exceptions.technical import TechnicalError
from giraffe.helpers import log_helper


class ConfigHelper:
    default_configurations_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'configuration/defaults.ini'))

    def __init__(self, configurations_ini_file_path: str = default_configurations_file):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        if not os.path.isfile(configurations_ini_file_path):
            raise TechnicalError(f'{configurations_ini_file_path} does not exist.')

        neo4j_section = 'NEO4J'
        redis_section = 'REDIS'
        testing_section = 'TESTING'
        giraffe_section = 'GIRAFFE'
        general_section = 'GENERAL'
        spark_section = 'SPARK'
        elastic_section = 'ELASTICSEARCH'

        config_file_path = os.path.abspath(configurations_ini_file_path)
        self.log.info(f'Configuration file: {config_file_path}')
        self.config = configparser.ConfigParser()

        self.config.read(configurations_ini_file_path)

        self.log.debug(f'Found the following configuration sections: {self.config.sections()}')

        # General settings

        if self.config.has_section(section=neo4j_section):
            self.string_encoding = self.config[general_section]['string_encoding']
        else:
            self.log.warning(f'No configuration found for section {general_section}')

        # Reading Neo4j connection details from configuration-file

        if self.config.has_section(section=neo4j_section):
            self.neo_host_address = self.config[neo4j_section]['HOST']
            self.neo_username = self.config[neo4j_section]['USERNAME']
            self.neo_password = self.config[neo4j_section]['PASSWORD']
            self.neo_bolt_port = self.config[neo4j_section]['BOLT_PORT']
        else:
            self.log.warning(f'No configuration found for section {neo4j_section}')

        self.neo_bolt_uri = f'bolt://{self.neo_host_address}:{self.neo_bolt_port}'

        if self.config.has_section(section=redis_section):
            # Reading REDIS connection details from configuration-file
            self.redis_host_address = self.config[('%s' % redis_section)]['HOST']
            self.redis_username = self.config[redis_section]['USERNAME']
            self.redis_password = self.config[redis_section]['PASSWORD']
            self.redis_port = self.config[redis_section]['PORT']
        else:
            self.log.warning(f'No configuration found for section {redis_section}')

        if self.config.has_section(section=testing_section):
            # Unit-Testing settings
            self.test_labels = self.config[testing_section]['test_labels'].split(',')
            self.test_edge_type = self.config[testing_section]['test_edge_type']
            self.test_property = self.config[testing_section]['test_property']
            self.number_of_test_nodes = int(self.config[testing_section]['number_of_test_nodes'])
            self.number_of_test_edges = int(self.config[testing_section]['number_of_test_edges'])
            self.test_chunk_size = int(self.config[testing_section]['test_chunk_size'])
            self.test_job_name = self.config[testing_section]['test_request_name']
            self.test_elasticsearch_index = self.config[testing_section]['test_elasticsearch_index']

        else:
            self.log.warning(f'No configuration found for section {testing_section}')

        if self.config.has_section(section=giraffe_section):
            # Giraffe logic
            self.nodes_ingestion_operation = self.config[giraffe_section]['nodes_ingestion_operation']
            self.edges_ingestion_operation = self.config[giraffe_section]['edges_ingestion_operation']
            self.key_separator = self.config[giraffe_section]['key_separator']
            self.uid_property = self.config[giraffe_section]['unique_identifier_property_name']
            self.from_uid_property = self.config[giraffe_section]['from_uid_property_name']
            self.to_uid_property = self.config[giraffe_section]['to_uid_property_name']
            self.edge_type_property = self.config[giraffe_section]['edge_type_property_name']
        else:
            self.log.warning(f'No configuration found for section {giraffe_section}')

        if self.config.has_section(section=spark_section):
            # Spark logic
            self.external_jars_folder = self.config[spark_section]['external_jars']
            self.spark_app_name = self.config[spark_section]['app_name']
        else:
            self.log.warning(f'No configuration found for section {spark_section}')

        if self.config.has_section(section=elastic_section):
            # ElasticSearch logic
            self.es_host_address = self.config[elastic_section]['HOST']
        else:
            self.log.warning(f'No configuration found for section {elastic_section}')
