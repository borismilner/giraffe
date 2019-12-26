import configparser
import os
from typing import Dict

from giraffe.helpers import log_helper
from giraffe.helpers.utilities import validate_is_file


class ConfigHelper:
    default_configurations_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'configuration/defaults.ini'))

    def __init__(self, configurations_ini_file_path: str = default_configurations_file):
        self.log = log_helper.get_logger(logger_name=__name__)

        validate_is_file(file_path=configurations_ini_file_path)

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
            self.redis_stream_milliseconds_block = self.config[redis_section]['STREAM_BLOCK_MILLISECONDS']
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
            self.test_redis_table_prefix = self.config[testing_section]['test_redis_table_prefix']
            self.test_redis_stream_name = self.config[testing_section]['test_redis_stream_name']
            self.test_front_desk_address = self.config[testing_section]['test_front_desk_address']

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
            self.deletion_batch_size = self.config[giraffe_section]['deletion_batch_size']
            self.model_vertex_id_prop_name = self.config[giraffe_section]['model_vertex_id_prop_name']
            self.model_edge_id_prop_name = self.config[giraffe_section]['model_edge_id_prop_name']
            self.edge_to_identifier_name = self.config[giraffe_section]['edge_to_identifier_name']
            self.inception_models_rest_address = self.config[giraffe_section]['inception_models_rest_address']
            self.inception_data_sources_rest_address = self.config[giraffe_section]['inception_data_sources_rest_address']
            self.data_source_parts_separator = self.config[giraffe_section]['data_source_parts_separator']
            self.expected_number_of_source_parts = int(self.config[giraffe_section]['expected_number_of_source_parts'])
            self.front_desk_port = int(self.config[giraffe_section]['front_desk_port'])
            self.redis_stream_name = self.config[giraffe_section]['redis_stream_name']
            self.ingestion_endpoint = self.config[giraffe_section]['ingestion_endpoint'].strip()
            self.redis_get_all_endpoint = self.config[giraffe_section]['redis_get_all_endpoint'].strip()
            self.request_mandatory_field_names = eval(self.config[giraffe_section]['request_type_mandatory_field_name'])
            self.logs_storage_folder = self.config[giraffe_section]['logs_storage_folder']
            self.admin_db_table_name = self.config[giraffe_section]['admin_db_table_name']
            self.required_request_fields: Dict[str, Dict] = eval(self.config[giraffe_section]['required_request_fields'])
            self.hash_uid_column = eval(self.config[giraffe_section]['hash_uid_column'])
            self.front_desk_ip = self.config[giraffe_section]['front_desk_ip']
            self.execution_environment = self.config[giraffe_section]['execution_environment']
            self.logs_structured_prefix = self.config[giraffe_section]['logs_structured_prefix']
            self.logs_fluentd_host = self.config[giraffe_section]['logs_fluentd_host']
            self.logs_fluentd_port = int(self.config[giraffe_section]['logs_fluentd_port'])
            self.thread_pool_size = int(self.config[giraffe_section]['thread_pool_size'])
            self.property_names_to_index = eval(self.config[giraffe_section]['property_names_to_index'])
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


config = None


def get_config(configurations_ini_file_path=None) -> ConfigHelper:
    global config
    if config is not None:
        return config
    if configurations_ini_file_path is None:
        config = ConfigHelper()
    else:
        config = ConfigHelper(configurations_ini_file_path=configurations_ini_file_path)
    return config
