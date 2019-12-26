import argparse
import atexit
import json
import os
import sys
from datetime import datetime
from json import JSONDecodeError
from logging.handlers import RotatingFileHandler
from typing import List

from flask import abort
from flask import Flask
from flask import request
from giraffe.business_logic.abstract.data_to_graph_translation_provider import DataToGraphEntitiesProvider
from giraffe.business_logic.coordinator import Coordinator
from giraffe.business_logic.data_to_entities_translators.mock_translator import MockDataToGraphEntitiesProvider
from giraffe.data_access.abstract.data_and_model_provider import DataAndModelProvider
from giraffe.data_access.data_model_providers.mock_data_model_provider import MockDataAndModelProvider
from giraffe.data_access.redis_db import RedisDB
from giraffe.helpers import config_helper
from giraffe.helpers import log_helper
from giraffe.helpers.structured_logging_fields import Field
from giraffe.helpers.utilities import validate_is_file
from waitress import serve

coordinator: Coordinator

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Front Desk')
    parser.add_argument('--config_ini', type=str, required=False, default=None)
    args = parser.parse_known_args()

    configuration_ini_file_path = args[0].config_ini
    if configuration_ini_file_path is None:
        config = config_helper.get_config()
    else:
        config_ini_file = configuration_ini_file_path
        validate_is_file(file_path=config_ini_file)
        config = config_helper.get_config(configurations_ini_file_path=config_ini_file)

    logging_file_path = os.path.join(config.logs_storage_folder, 'giraffe.log')
    file_handler = RotatingFileHandler(filename=logging_file_path,
                                       mode='a',
                                       maxBytes=1_000_000_000,
                                       backupCount=3,
                                       encoding='utf-8',
                                       delay=False)
    file_handler.setFormatter(log_helper.log_row_format)
    log_helper.add_handler(handler=file_handler)

    atexit.register(log_helper.stop_listener)

    log = log_helper.get_logger(__name__)

    execution_env = config.execution_environment

    log.info(f'Execution environment: {execution_env}')
    log.admin({Field.execution_environment: execution_env})

    # Instantiating providers based on the execution environment

    if execution_env == 'dev':
        data_and_model_provider: DataAndModelProvider = MockDataAndModelProvider()
        data_to_graph_entities_provider: DataToGraphEntitiesProvider = MockDataToGraphEntitiesProvider()
    elif execution_env == 'cortex':
        raise NotImplementedError('Implemented in cortex')
    else:
        log.info(f'Unexpected value in configuration file for execution_environment: {execution_env}')
        sys.exit(1)

    not_acceptable_error_code = 406

    log.debug('Initializing coordinator module.')
    coordinator = Coordinator(config_helper=config,
                              data_and_model_provider=data_and_model_provider,
                              data_to_graph_translator=data_to_graph_entities_provider)
    if not coordinator.is_ready:
        log.error('Failed initializing coordinator component - aborting.')
        sys.exit(-1)

    app = Flask(__name__)

    supported_request_types = [key for key in config.required_request_fields.keys()]


    def get_invalid_fields(received_request: dict) -> List[str]:
        template = config.required_request_fields[received_request['request_type']]
        invalid_fields = []
        fields_missing_from_request = [key for key in template.keys() if key not in received_request]
        invalid_fields.extend([f'Field {key} is missing.' for key in fields_missing_from_request])
        incorrect_value_types = [key for key in template.keys() if (key in received_request and not isinstance(received_request[key], eval(template[key])))]
        invalid_fields.extend([f'Field {key} must be of type {template[key]}' for key in incorrect_value_types])
        return invalid_fields


    @app.route(config.ingestion_endpoint, methods=['POST'])
    def ingest():
        client_request = None
        try:
            client_request = json.loads(request.data, encoding='utf8')
        except (JSONDecodeError, TypeError, Exception):
            abort(not_acceptable_error_code, 'Failed parsing the request as a JSON string.')
        log.info(f'Received a request from {request.remote_addr}: {client_request}')
        log.admin({
                Field.request_ip: request.remote_addr,
                Field.request: client_request
        })
        request_type_field_names = config.request_mandatory_field_names

        # Start of validations
        for required_field_name in request_type_field_names:
            if required_field_name not in client_request:
                abort(not_acceptable_error_code,
                      f'Field {required_field_name} is mandatory for determining the type of the request and must be one of the following: {supported_request_types}')

        if client_request['request_type'] not in supported_request_types:
            abort(not_acceptable_error_code,
                  f'Unsupported request type: {request_type_field_names} — must be one of the following: {supported_request_types}')

        invalid_fields = get_invalid_fields(received_request=client_request)
        if len(invalid_fields) > 0:
            abort(not_acceptable_error_code, f'Please correct the following: {",".join(invalid_fields)}')
        # End of validations

        log.debug('Starting coordinator processing function.')
        coordinator.begin_processing(client_request=client_request)

        now = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
        return f"[{now}] Received: {client_request}."  # Success 200 OK


    @app.route(config.redis_get_all_endpoint, methods=['GET'])
    def all_redis_keys():
        r = RedisDB(config=config)
        all_keys = r.get_key_by_pattern(key_pattern='*', return_list=True)
        return f'All redis keys: {all_keys}'


    log.info(f'Front-Desk shall be available on port: {config.front_desk_port}')
    serve(app, host=config.front_desk_ip, port=config.front_desk_port)
