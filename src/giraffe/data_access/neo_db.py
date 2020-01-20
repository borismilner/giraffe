import atexit
import threading
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

from giraffe.exceptions.logical import PropertyNotIndexedError
from giraffe.exceptions.logical import QuerySyntaxError
from giraffe.exceptions.technical import TechnicalError
from giraffe.helpers import config_helper
from giraffe.helpers import log_helper
from giraffe.helpers.EventDispatcher import EventDispatcher
from giraffe.helpers.EventDispatcher import GiraffeEvent
from giraffe.helpers.EventDispatcher import GiraffeEventType
from neo4j import BoltStatementResult
from neo4j import BoltStatementResultSummary
from neo4j import GraphDatabase
from neobolt.exceptions import CypherSyntaxError
from neobolt.exceptions import ServiceUnavailable
from py2neo import Graph


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class NeoDB(object):

    def __init__(self, event_dispatcher: EventDispatcher, config=config_helper.get_config()):
        self.config = config
        self.event_dispatcher = event_dispatcher
        self.log = log_helper.get_logger(logger_name=f'{self.__class__.__name__}_{threading.current_thread().name}')

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

        self.indices_cache_label_property: List[Tuple[str, str]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._driver.close()

    def is_index_exists(self, label: str, property_name: str):
        label_and_property = (label, property_name)
        if label_and_property in self.indices_cache_label_property:
            return True
        query = f'CALL db.indexes() YIELD tokenNames, properties WHERE "{label}" IN tokenNames AND "{property_name}" IN properties RETURN count(*) AS count'
        count = self.pull_query(query=query).value()[0]
        is_index_exist = count > 0
        if is_index_exist:
            self.indices_cache_label_property.append(label_and_property)
        return is_index_exist

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
        label_and_property = (label, property_name)
        if label_and_property in self.indices_cache_label_property:
            self.indices_cache_label_property.remove(label_and_property)
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

        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run(query)
                return result

    # NOTE: since UNWIND won't allow dynamic labels - all nodes in the batch must have the same label.
    def merge_nodes(self, nodes: List, request_id: str, label: str = None) -> BoltStatementResultSummary:
        # Notice the ON MATCH clause - it will add/update missing properties if there are such
        # Perhaps we don't care about adding and would want to simply overwrite the existing one with `=`
        # TODO: Consider saving date-time as epoch seconds/milliseconds

        for property_name in self.config.property_names_to_index['*']:
            self.create_index_if_not_exists(label=label, property_name=property_name)
        if label in self.config.property_names_to_index:
            for property_name in self.config.property_names_to_index[label]:
                self.create_index_if_not_exists(label=label, property_name=property_name)

        query = f"""
        UNWIND $nodes as node
        MERGE (p:{label}{{{self.config.uid_property}: node.{self.config.uid_property}}})
        ON CREATE SET p = node, p._created = datetime()
        ON MATCH SET p += node, p._last_updated = datetime()
        """
        self.log.debug(f'Pushing into neo4j {len(nodes)} nodes.')
        summary = self.run_query(query=query, nodes=nodes)
        self.log.debug(f'Done pushing: {summary.counters}')
        self.event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.PUSHED_GRAPH_ELEMENTS_INTO_NEO,
                        message=f'Pushed: {len(nodes)} nodes into neo4j: {str(label)} [{summary.counters}]',
                        arguments={'request_id': request_id,
                                   'element_type': 'NODES',
                                   'element_properties': str(label),
                                   'summary': summary
                                   }
                )
        )
        return summary

    # NOTE: while it is possible to match without the from/to labels - it is too slow.
    def merge_edges(self, edges: List, from_label: str, to_label: str, request_id: str, edge_type: str = None) -> BoltStatementResultSummary:
        if edge_type is None:
            edge_type = edges[0][self.config.edge_type_property]
        # TODO: Consider adding ON CREATE & ON MATCH after final MERGE
        query = f"""
        UNWIND $edges as edge
        MATCH (fromNode:{from_label}) WHERE fromNode.{self.config.uid_property} = edge.{self.config.from_uid_property}
        MATCH (toNode:{to_label}) WHERE toNode.{self.config.uid_property} = edge.{self.config.to_uid_property}
        MERGE (fromNode)-[r:{edge_type}]->(toNode)
        """

        self.log.debug(f'Pushing into neo4j {len(edges)} edges of type {edge_type}.')
        summary = self.run_query(query=query, edges=edges)
        self.log.debug(f'Done pushing: {summary.counters}')
        self.event_dispatcher.dispatch_event(
                event=GiraffeEvent(
                        request_id=request_id,
                        event_type=GiraffeEventType.PUSHED_GRAPH_ELEMENTS_INTO_NEO,
                        message=f'Pushed: {len(edges)} edges into neo4j: {edge_type}_{from_label}_{to_label} [{summary.counters}]',
                        arguments={'request_id': request_id,
                                   'element_type': 'EDGES',
                                   'element_properties': f'{edge_type}_{from_label}_{to_label}',
                                   'summary': summary
                                   }
                )
        )
        return summary

    def delete_nodes_by_properties(self, label: str, property_name_value_tuples: List[Tuple[str, str]]) -> Dict[str, int]:

        for name_value in property_name_value_tuples:
            property_name = name_value[0]
            if not self.is_index_exists(label=label, property_name=property_name):
                raise PropertyNotIndexedError(f'Property {property_name} must be indexed in-order to delete nodes by it.')

        conditions_string = ' and '.join([f"n.{name_value[0]}='{name_value[1]}'" for name_value in property_name_value_tuples])

        query = f"""
        call apoc.periodic.iterate("MATCH (n:{label}) where {conditions_string} return n", "DETACH DELETE n", {{batchSize:{int(self.config.deletion_batch_size)}}})
        yield batches, total return batches, total
        """

        result = next(self.pull_query(query=query).records())
        return {'batches': result['batches'], 'total': result['total']}
