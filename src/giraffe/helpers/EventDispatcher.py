import dill
from typing import Callable
from typing import List

from giraffe.helpers import log_helper
from giraffe.helpers.communicator import Communicator
from giraffe.helpers.communicator import CommunicatorMode
from giraffe.monitoring.giraffe_event import GiraffeEvent


class EventDispatcher:
    def __init__(self, external_monitoring_enabled: bool = True, monitoring_host: str = 'localhost', monitoring_port: int = 65432):
        self.listeners: List[Callable] = []
        self.log = log_helper.get_logger(logger_name='Event-Dispatcher')
        self.tcp_server: Communicator = Communicator(mode=CommunicatorMode.SERVER, host=monitoring_host, port=monitoring_port)
        self.external_monitoring_enabled = external_monitoring_enabled
        if self.external_monitoring_enabled:
            self.tcp_server.start()

    def dispatch_event(self, event: GiraffeEvent):
        self.log.debug(event.message)
        for callback in self.listeners:
            try:
                callback(event)
            except Exception as exception:  # Assuming it's the fault of callback-author
                self.log.warning(f'Failed calling a callback function: {exception}')

        if self.external_monitoring_enabled and len(self.tcp_server.listeners) > 0:
            # noinspection PyBroadException
            try:
                message = f'{dill.dumps(event).hex()}'
            except:  # Some events are non-pickle-friendly
                # TODO: Modify only the non-pickle-friendly fields
                alternative_arguments = {key: 'NOT PICKLED' for key in event.arguments.keys()}
                alternative_event = GiraffeEvent(request_id=event.request_id,
                                                 event_type=event.event_type,
                                                 message=event.message,
                                                 arguments=alternative_arguments)
                message = f'{dill.dumps(alternative_event).hex()}'
            self.tcp_server.broadcast_to_clients(data=message)

    def register_callback(self, callback: Callable):
        self.listeners.append(callback)
