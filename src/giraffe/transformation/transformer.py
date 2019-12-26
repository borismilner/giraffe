from typing import List, Dict
import giraffe.transformation.model_constants as mc
from giraffe.exceptions.logical import ParseModelFailure, TransformerError
from giraffe.transformation.model_utilities import validate_has_attribute, get_vertex_label_by_id, get_edge_id, \
    extract_identifier_as_vertex
from giraffe.transformation.edge import Edge
from giraffe.transformation.vertex import Vertex
from pyspark.sql import DataFrame
from giraffe.helpers import config_helper


class Transformer:
    def __init__(self):
        self._result = None
        self._config = config_helper.get_config()

    def transform(self, src_dataframe: DataFrame, model: dict, key_prefix: str) -> Dict[str, List[DataFrame]]:
        validate_has_attribute(model, mc.UNIPOP, mc.MODEL, mc.MODEL)
        validate_has_attribute(model[mc.UNIPOP], mc.VERTICES, mc.UNIPOP)
        if src_dataframe is None:
            raise TransformerError(f"can't transform source {key_prefix}, source dataframe is None")
        self._result = {}
        vertices = model[mc.UNIPOP][mc.VERTICES]
        for vertex_model in vertices:
            vertex = Vertex(vertex_model, src_dataframe)
            vertex_dataframe = vertex.compute_dataframe()
            self._add_to_result(self._compute_vertex_key(key_prefix, vertex_model), vertex_dataframe)
            if mc.EDGES in vertex_model and isinstance(vertex_model[mc.EDGES], list):
                edges = vertex_model[mc.EDGES]
                for edge_model in edges:
                    self._add_possible_identifier(edge_model, src_dataframe, key_prefix)
                    edge = Edge(edge_model, vertex_dataframe)
                    edge_dataframe = edge.compute_dataframe()
                    self._add_to_result(self._compute_edge_key(key_prefix, edge_model, vertex_model, model), edge_dataframe)
        return self._result

    def _add_to_result(self, key, dataframe):
        if key not in self._result:
            self._result[key] = []
        self._result[key].append(dataframe)

    def _compute_vertex_key(self, key_prefix, vertex_model):
        validate_has_attribute(vertex_model, mc.LABEL, mc.VERTEX)
        key = f"{key_prefix}:{self._config.nodes_ingestion_operation}:{vertex_model[mc.LABEL]}"
        return key

    def _compute_edge_key(self, key_prefix, edge_model, containing_vertex_model, model):
        validate_has_attribute(edge_model, mc.LABEL, mc.EDGE)
        validate_has_attribute(edge_model, mc.DIRECTION, mc.EDGE)
        edge_id = get_edge_id(edge_model)
        if edge_model[mc.DIRECTION] == mc.OUT:
            from_label = containing_vertex_model[mc.LABEL]
            to_label = get_vertex_label_by_id(model, edge_id)
        elif edge_model[mc.DIRECTION] == mc.IN:
            from_label = mc.IDENTIFIER if edge_model[mc.LABEL] == mc.IDENTIFIES else \
                get_vertex_label_by_id(model, edge_id)
            to_label = containing_vertex_model[mc.LABEL]
        else:
            raise ParseModelFailure(f"illegal value for edge direction: {edge_model[mc.DIRECTION]}")
        key = f"{key_prefix}:{self._config.edges_ingestion_operation}:{edge_model[mc.LABEL]},{from_label},{to_label}"
        return key

    def _add_possible_identifier(self, edge_model, src_dataframe, key_prefix):
        if edge_model[mc.LABEL] != mc.IDENTIFIES:
            return
        identifier_vertex_model = extract_identifier_as_vertex(edge_model)
        identifier_vertex = Vertex(identifier_vertex_model, src_dataframe)
        identifier_vertex_dataframe = identifier_vertex.compute_dataframe()
        self._add_to_result(self._compute_vertex_key(key_prefix, identifier_vertex_model), identifier_vertex_dataframe)
