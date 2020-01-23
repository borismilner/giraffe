import time
from concurrent.futures import ThreadPoolExecutor

from giraffe.helpers.communicator import Communicator
from giraffe.helpers.communicator import CommunicatorMode
from giraffe.helpers.multi_helper import MultiHelper

test_port = 5555


def test_communicator():
    server = Communicator(mode=CommunicatorMode.SERVER, port=test_port)
    client = Communicator(mode=CommunicatorMode.CLIENT, port=test_port)
    for i in range(0, 100):
        server.broadcast_to_clients(data=f'Message:{i}')

    messages = client.message_iterator(timeout_seconds=2)

    for index, message in enumerate(messages):
        assert message == f'Message:{index}'
