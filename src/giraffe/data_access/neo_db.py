import atexit
from typing import List, Union
from giraffe.exceptions.logical import QuerySyntaxError, PropertyNotIndexedError
from giraffe.helpers.config_helper import ConfigHelper
from neo4j import GraphDatabase, BoltStatementResultSummary, BoltStatementResult
from neobolt.exceptions import ServiceUnavailable, CypherSyntaxError

from giraffe.exceptions.technical import TechnicalError
from giraffe.helpers import log_helper
from py2neo import Graph


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class NeoDB(object):

    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.config = config
        self.log = log_helper.get_logger(logger_name=self.__class__.__name__)

        # Connecting py2neo

        self.graph = Graph(
            uri=config.neo_host_address,
            user=config.neo_username,
            password=config.neo_password
        )

        # Connecting official bolt-driver

        self._driver = GraphDatabase.driver(uri=config.neo_bolt_uri,
                                            auth=(config.neo_username, config.neo_password))

        try:
            db_kernel_start = self.graph.database.kernel_start_time
        except ServiceUnavailable as _:
            raise TechnicalError(f'Neo4j does not seem to be active at {config.neo_host_address}')
        self.log.debug(f'Neo4j is active since {db_kernel_start}.')

        atexit.register(self._driver.close)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._driver.close()

    def is_index_exists(self, label: str, property_name: str):
        query = f'CALL db.indexes() YIELD tokenNames, properties WHERE "{label}" IN tokenNames AND "{property_name}" IN properties RETURN count(*) AS count'
        count = self.pull_query(query=query).value()[0]
        return count > 0

    def create_index_if_not_exists(self, label: str, property_name: str) -> Union[BoltStatementResultSummary, None]:

        if self.is_index_exists(label=label, property_name=property_name):
            return None
        self.log.info(f'Creating index on {label}.{property_name}')
        query = f'CREATE INDEX ON :{label}({property_name})'
        summary = self.run_query(query=query)
        return summary

    def drop_index_if_exists(self, label: str, property_name: str) -> Union[BoltStatementResultSummary, None]:
        if not self.is_index_exists(label=label, property_name=property_name):
            self.log.warning(f'Will not drop index on {label}.{property_name} since is does not exist.')
            return None
        query = f'DROP INDEX ON :{label}({property_name})'
        summary: BoltStatementResultSummary = self.run_query(query=query)
        indexes_removed = summary.counters.indexes_removed
        self.log.debug(f'Dropped {indexes_removed} index: {label}.{property_name}]')
        return summary

    def run_query(self, query: str, **parameters) -> BoltStatementResultSummary:
        summary: BoltStatementResultSummary
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run(query, **parameters)
                try:
                    summary = result.consume()
                except CypherSyntaxError as e:
                    tx.success = False
                    tx.close()
                    raise QuerySyntaxError(e)
                tx.success = True
        return summary

    def pull_query(self, query: str) -> BoltStatementResult:

        summary: BoltStatementResult
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run(query)
                return result

    # NOTE: since UNWIND won't allow dynamic labels - all nodes in the batch must have the same label.
    def merge_nodes(self, nodes: List, label: str = None) -> BoltStatementResultSummary:
        # Notice the ON MATCH clause - it will add/update missing properties if there are such
        # Perhaps we don't care about adding and would want to simply overwrite the existing one with `=`
        # TODO: Consider saving date-time as epoch seconds/milliseconds

        self.create_index_if_not_exists(label=label, property_name=self.config.uid_property)
        query = f"""
        UNWIND $nodes as node
        MERGE (p:{label}{{{self.config.uid_property}: node.{self.config.uid_property}}})
        ON CREATE SET p = node, p._created = datetime()
        ON MATCH SET p += node, p._last_seen = datetime()
        """
        summary = self.run_query(query=query, nodes=nodes)
        return summary

    # NOTE: while it is possible to match without the from/to labels - it is too slow.
    def merge_edges(self, edges: List, from_label: str, to_label: str, edge_type: str = None) -> BoltStatementResultSummary:
        if edge_type is None:
            edge_type = edges[0][self.config.edge_type_property]
        query = f"""
        UNWIND $edges as edge
        MATCH (fromNode:{from_label}) WHERE fromNode.{self.config.uid_property} = edge.{self.config.from_uid_property}
        MATCH (toNode:{to_label}) WHERE toNode.{self.config.uid_property} = edge.{self.config.from_uid_property}
        MERGE (fromNode)-[r:{edge_type}]->(toNode)
        """

        summary = self.run_query(query=query, edges=edges)
        return summary

    def delete_nodes_by_property(self, label: str, property_name: str, property_value: str):
        if not self.is_index_exists(label=label, property_name=property_name):
            raise PropertyNotIndexedError(f'Property {property_name} must be indexed in-order to delete nodes by it.')
        query = f"""
        MATCH(n:{label} {{{property_name}: {property_value if property_value.isdigit() else "'" + property_value + "'"}}})
        DETACH DELETE n
        """

        summary = self.run_query(query=query)
        return summary
