import os

import requests
from giraffe.helpers.config_helper import ConfigHelper

import tests.business_logic as bl

config: ConfigHelper
status_code_unacceptable = 406

white_list_file_path = os.path.join(os.path.dirname(os.path.abspath(bl.__file__)), "white_list.txt")


def test_ingest_request(config_helper, logger):
    log = logger
    front_desk_address = f'{config_helper.test_front_desk_address}:{config_helper.front_desk_port}'
    ingest_address = f'{front_desk_address}{config_helper.ingestion_endpoint}'
    data = {}

    def get_reply(address: str = ingest_address) -> requests.models.Response:
        nonlocal data
        the_reply = requests.post(address, json=data)
        return the_reply

    reply = get_reply(front_desk_address)
    log.debug('Accessing the REST address without a specific endpoint')
    assert reply.status_code == 404

    reply = get_reply(ingest_address)
    assert reply.status_code == status_code_unacceptable  # No request type field
    assert config_helper.request_mandatory_field_names[0] in reply.text

    data = {'name': 'boris'}  # Still no request type field
    reply = get_reply()
    assert reply.status_code == status_code_unacceptable
    assert config_helper.request_mandatory_field_names[0] in reply.text

    data = {"request_type": "nothing", "request_id": '123'}  # Request type exists by the request is unsupported
    reply = get_reply()
    assert reply.status_code == status_code_unacceptable
    assert 'unsupported' in reply.text.lower()

    data = {"request_type": "white_list", "request_id": '123'}  # Request parameters not supplied
    reply = get_reply()
    assert reply.status_code == status_code_unacceptable
    assert 'missing' in reply.text.lower()

    data = {"request_type": "white_list", "file_path": 1, "request_id": '123'}  # Request parameters supplied but with a wrong type
    reply = get_reply()
    assert reply.status_code == status_code_unacceptable
    assert 'must be of type' in reply.text.lower()

    data = {"request_type": "white_list", "file_path": white_list_file_path, "request_id": '123'}  # Goodie — this is what we expect according the configuration in the test
    reply = get_reply()
    assert reply.status_code == 200
