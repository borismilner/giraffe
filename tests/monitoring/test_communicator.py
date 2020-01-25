from giraffe.helpers.communicator import Communicator
from giraffe.helpers.communicator import CommunicatorMode

test_port = 5555


# TODO: Investigate: May fail with "OSError: [WinError 10022] An invalid argument was supplied" when run with other tests
def test_communicator():
    server = Communicator(mode=CommunicatorMode.SERVER, port=test_port)
    client = Communicator(mode=CommunicatorMode.CLIENT, port=test_port)
    for i in range(0, 100):
        server.broadcast_to_clients(data=f'Message:{i}')

    messages = client.message_iterator(timeout_seconds=2)

    for index, message in enumerate(messages):
        assert message == f'Message:{index}'
