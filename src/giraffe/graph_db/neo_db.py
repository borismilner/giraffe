from neobolt.exceptions import ServiceUnavailable

from giraffe.exceptions.technical_error import TechnicalError
from giraffe.helpers import log_helper
from py2neo import Graph


class NeoDB:
    def __init__(self, host_address: str, username: str, password: str):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)
        self.graph = Graph(uri=host_address, user=username, password=password)
        try:
            db_kernel_start = self.graph.database.kernel_start_time
        except ServiceUnavailable as _:
            raise TechnicalError(f'Neo4j does not seem to be active at {host_address}')
        self.log.debug(f'Neo4j is active since {db_kernel_start}.')
