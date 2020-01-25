import argparse
import atexit
import json
import os
import sys
from datetime import datetime
from json import JSONDecodeError
from typing import List

from flask import abort
from flask import Flask
from flask import request
from giraffe.business_logic.env_provider import EnvProvider
from giraffe.business_logic.coordinator import Coordinator
from giraffe.data_access.redis_db import RedisDB
from giraffe.helpers import log_helper
from giraffe.helpers.EventDispatcher import EventDispatcher
from giraffe.helpers.utilities import timestamp_to_str
from giraffe.monitoring.giraffe_event import GiraffeEvent
from giraffe.monitoring.giraffe_event import GiraffeEventType
from giraffe.monitoring.progress_monitor import ProgressMonitor

from waitress import serve

coordinator: Coordinator
progress_monitor: ProgressMonitor

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Front Desk')
    parser.add_argument('--config_ini', type=str, required=False, default=None)
    args = parser.parse_known_args()

    env = EnvProvider(cmd_line_args=args)

    event_dispatcher = EventDispatcher()
    log = log_helper.get_logger(__name__)

    log.info(f'Execution environment: {env.execution_env}')

    not_acceptable_error_code = 406

    log.debug('Initializing coordinator module.')
    progress_monitor = ProgressMonitor(event_dispatcher=event_dispatcher, config=env.config)
    atexit.register(progress_monitor.dump_and_clear_memory)
    coordinator = Coordinator(env=env,
                              progress_monitor=progress_monitor,
                              event_dispatcher=event_dispatcher)
    if not coordinator.is_ready:
        log.error('Failed initializing coordinator component - aborting.')
        sys.exit(-1)

    atexit.register(coordinator.multi_helper.thread_executor.shutdown)

    app = Flask(__name__)

    supported_request_types = [key for key in env.config.required_request_fields.keys()]


    def get_invalid_fields(received_request: dict) -> List[str]:
        template = env.config.required_request_fields[received_request['request_type']]
        invalid_fields = []
        fields_missing_from_request = [key for key in template.keys() if key not in received_request]
        invalid_fields.extend([f'Field {key} is missing.' for key in fields_missing_from_request])
        incorrect_value_types = [key for key in template.keys() if (key in received_request and not isinstance(received_request[key], eval(template[key])))]
        invalid_fields.extend([f'Field {key} must be of type {template[key]}' for key in incorrect_value_types])
        return invalid_fields


    @app.route(env.config.ingestion_endpoint, methods=['POST'])
    def ingest():
        client_request = None
        try:
            client_request = json.loads(request.data, encoding='utf8')
        except (JSONDecodeError, TypeError, Exception):
            failure_message = 'Failed parsing the request as a JSON string.'
            event_dispatcher.dispatch_event(
                    event=GiraffeEvent(
                            request_id=None,
                            event_type=GiraffeEventType.GENERAL_ERROR,
                            message=failure_message,
                            arguments={
                                    'client_ip': request.remote_addr,
                                    'request_data': request.data
                            }
                    )
            )
            abort(not_acceptable_error_code, failure_message)

        request_received_message = f'Received a request from {request.remote_addr}: {client_request}'
        request_id = f"{client_request['request_id']}" if 'request_id' in client_request else 'UNASSIGNED'
        event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.RECEIVED_REQUEST,
                        message=request_received_message,
                        arguments={
                                'client_ip': request.remote_addr,
                                'request_content': client_request
                        }
                )
        )

        request_type_field_names = env.config.request_mandatory_field_names

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
        coordinator.thread_pool.apply_async(coordinator.process_request,
                                            (client_request,),
                                            callback=coordinator.processing_success_callback,
                                            error_callback=coordinator.processing_error_callback)

        now = timestamp_to_str(timestamp=datetime.now())
        return f"[{now}] Received: {client_request}."  # Success 200 OK


    @app.route(env.config.redis_get_all_endpoint, methods=['GET'])
    def all_redis_keys():
        r = RedisDB(event_dispatcher=event_dispatcher, env=env)
        all_keys = r.get_key_by_pattern(key_pattern='*', return_list=True)
        return f'All redis keys: {all_keys}'


    @app.route('/progress', methods=['GET'])
    def get_progress_report():
        request_id = request.args.get('request_id')
        task = progress_monitor.get_task(task_id=request_id)
        if task is None:
            file_path = os.path.join(env.config.progress_monitor_dump_folder, request_id)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as json_on_hd:
                    return json_on_hd.read()
            else:
                abort(404, f'Request not found - neither in memory nor on hard-drive.')
        return task.as_json()


    @app.route('/clear_progress', methods=['GET'])
    def clear_progress_and_dump():
        progress_monitor.dump_and_clear_memory()
        abort(200, 'Done.')


    @app.route('/ping', methods=['GET'])
    def ping():
        abort(200, 'Pong')


    log.info(f'Front-Desk shall be available on port: {env.config.front_desk_port}')
    serve(app, host=env.config.front_desk_ip, port=env.config.front_desk_port)
