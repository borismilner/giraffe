from giraffe.helpers.config_helper import ConfigHelper
from neo4j import GraphDatabase
from neobolt.exceptions import ServiceUnavailable

from giraffe.exceptions.technical_error import TechnicalError
from giraffe.helpers import log_helper
from py2neo import Graph


class NeoDB(object):

    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)

        # Connecting py2neo

        self.graph = Graph(
            uri=config.host_address,
            user=config.username,
            password=config.password
        )

        # Connecting official bolt-driver

        self._driver = GraphDatabase.driver(uri=config.bolt_uri,
                                            auth=(config.username, config.password))

        try:
            db_kernel_start = self.graph.database.kernel_start_time
        except ServiceUnavailable as _:
            raise TechnicalError(f'Neo4j does not seem to be active at {config.host_address}')
        self.log.debug(f'Neo4j is active since {db_kernel_start}.')

    def close(self):
        self._driver.close()
