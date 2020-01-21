import time
from concurrent.futures import ThreadPoolExecutor

from giraffe.helpers.communicator import Communicator
from giraffe.helpers.communicator import CommunicatorMode
from giraffe.helpers.multi_helper import MultiHelper

test_port = 5555
server: Communicator


def start_monitoring_server():
    global server
    server = Communicator(mode=CommunicatorMode.SERVER, port=test_port)
    server.start()
    for i in range(0, 100):
        server.broadcast_to_clients(data=f'Message:{i}')


def test_communicator():
    thread_pool = ThreadPoolExecutor(max_workers=3)
    future = thread_pool.submit(start_monitoring_server)
    time.sleep(1)
    client = Communicator(mode=CommunicatorMode.CLIENT, port=test_port)
    client.start()

    messages = client.message_iterator(timeout_seconds=2)

    for index, message in enumerate(messages):
        assert message == f'Message:{index}'

    future_result = MultiHelper.wait_on_futures([future])
    assert future_result.all_ok
