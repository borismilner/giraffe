import atexit
import pickle
import select
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue, Empty
import socket
import threading

from giraffe.helpers import log_helper
from enum import Enum, auto

from giraffe.monitoring.giraffe_event import GiraffeEvent

buffer_size = 10240


class CommunicatorMode(Enum):
    CLIENT = auto()
    SERVER = auto()


class Communicator:
    def __init__(self, mode: CommunicatorMode, host: str = 'localhost', port: int = 65432):
        self.lock = threading.Lock()
        self._host = host
        self._port = port
        self._mode = mode
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self._mode == CommunicatorMode.CLIENT:
            self._is_client = True
            self.socket.settimeout(30)  # seconds
            self.log = log_helper.get_logger(logger_name='Client-Communicator')
            self.buffer = bytearray
        else:
            self._is_client = False
            self.thread_pool = ThreadPoolExecutor(max_workers=2)
            self.listeners = []
            self.log = log_helper.get_logger(logger_name='Server-Communicator')
        atexit.register(self.stop)
        self.__start()

    def __start(self):
        connection_host_and_port = (self._host, self._port)
        if self._is_client:
            self.log.info(f'Client is connecting to host {self._host}, port {self._port}')
            self.socket.connect(connection_host_and_port)
            self.socket.sendall(b'Hello, world')
        else:
            self.log.info(f'Server is available at host {self._host}, port {self._port}')
            self.socket.bind(connection_host_and_port)
            self.socket.listen()
            self.thread_pool.submit(self.__accept_connection)

    def __accept_connection(self):
        while True:
            with self.lock:
                self.log.info('Awaiting connection from a TCP client.')
                client_socket, address = self.socket.accept()
                self.log.info(f'Accepted connection from {address}')
                self.listeners.append(client_socket)

    def stop(self):
        self.socket.close()

    def broadcast_to_clients(self, data):
        readable, writable, exceptional = select.select(self.listeners, [], [])
        for closed_client_socket in exceptional:  # Remove disconnected client-sockets
            self.listeners.remove(closed_client_socket)
        for client_socket in self.listeners:
            client_socket.sendall(bytes(data + '\r\n', encoding='utf-8'))

    # TODO: Refactor
    def message_iterator(self, timeout_seconds: int):
        q = Queue()
        buffer = bytes()
        while True:
            ready = select.select([self.socket], [], [], timeout_seconds)
            if ready[0]:
                buffer += self.socket.recv(buffer_size)
            else:
                while not q.empty():
                    yield q.get().decode(encoding='utf-8')
                return
            messages = buffer.split(b'\r\n')
            partial_end = messages[-1] != b''

            for message in messages[0:-1]:
                q.put(message)
            buffer = bytes() if not partial_end else messages[-1]
            if partial_end and q.empty() and len(messages) == 1:
                continue
            try:
                message = q.get().decode(encoding='utf-8')
                yield message
            except Empty:
                return

    def events_iterator(self, request_id: str, timeout_seconds: int):
        message_iterator = self.message_iterator(timeout_seconds=timeout_seconds)
        for message in message_iterator:
            event: GiraffeEvent = pickle.loads(bytes.fromhex(message))
            if request_id not in event.request_id:
                continue  # Ignore unread messages TODO: check for corner cases
            yield event

    def set_client_timeout_seconds(self, seconds: int):
        self.socket.settimeout(seconds)
