import pickle
import socket

import time
import threading
import requests
from giraffe.helpers import log_helper

buffer_size = 10240


class Communicator:
    def __init__(self, host: str = 'localhost', port: int = 65432):
        self.host = host
        self.port = port
        self.log = log_helper.get_logger(logger_name='Communicator')
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket.settimeout(30)  # seconds
        self.listeners = []

    def start_server(self):
        self.server_socket.bind(('localhost', self.port))
        time.sleep(1)
        self.log.info(f'Server is listening: host={self.host},port={self.port}')
        self.log.name = 'Server-Communicator'

    @staticmethod
    def request_monitor_acceptance():
        result = requests.get('http://localhost:9001/register_monitor')
        return result

    def start_client(self):
        t = threading.Thread(target=self.request_monitor_acceptance)
        t.start()
        time.sleep(2)
        self.client_socket.sendto(b'Hello', (self.host, self.port))
        t.join()
        self.log.name = 'Client-Communicator'

    def accept_client_connection(self):
        message, client_address = self.server_socket.recvfrom(buffer_size)
        self.log.info(f'Accepted client connection from {client_address}')
        self.listeners.append(client_address)

    def broadcast_to_clients(self, data):
        for client_address in self.listeners:
            self.client_socket.sendto(bytes(data, encoding='utf-8'), client_address)

    def receive_data(self):
        data = self.client_socket.recvfrom(buffer_size)
        return data[0].decode("utf-8")

    def fetch_event(self):
        try:
            data = self.client_socket.recvfrom(buffer_size)
            event = pickle.loads(bytes.fromhex(data[0].decode("utf-8")))
            return event
        except socket.timeout:
            self.log.error('Failed fetching data from a socket.')
            return None

    def set_client_timeout_seconds(self, seconds: int):
        self.client_socket.settimeout(seconds)
