from giraffe.exceptions.logical import TransformerError, ParseModelFailure
from giraffe.helpers import config_helper
from giraffe.transformation.model_utilities import validate_has_attribute, get_edge_id, extract_identifier_as_vertex
from giraffe.transformation.spark_utilities import rename_column, get_column_names, remove_columns
from pyspark.sql import DataFrame
import giraffe.transformation.model_constants as mc
from giraffe.transformation.property import Property


class Edge:
    def __init__(self, edge_model: dict, containing_vertex_dataframe: DataFrame) -> DataFrame:
        self._containing_vertex_dataframe = containing_vertex_dataframe
        self._edge_model = edge_model
        self._dataframe = None
        self._config = config_helper.get_config()

    def compute_dataframe(self):
        if self._dataframe is not None:
            return self._dataframe
        self._add_from_to()
        self._remove_unused_columns()
        return self._dataframe

    def _add_from_to(self):
        validate_has_attribute(mc.DIRECTION)
        direction = self._edge_model[mc.DIRECTION]
        if direction == mc.OUT:
            self._dataframe = rename_column(self._containing_vertex_dataframe,
                                            self._config.uid_property,
                                            self._config.from_uid_property)
            self._dataframe = Property(self._config.to_uid_property, get_edge_id(self._edge_model), self._dataframe) \
                    .get_updated_dataframe()
        elif direction == mc.IN:
            self._dataframe = rename_column(self._containing_vertex_dataframe,
                                            self._config.uid_property,
                                            self._config.to_uid_property)
            if self._config[mc.LABEL] == mc.IDENTIFIES:
                self._dataframe = Property(self._config.from_uid_property, extract_identifier_as_vertex(self.edge_model)[mc.ID],
                                           self._dataframe) \
                    .get_updated_dataframe()
            else:
                self._dataframe = Property(self._config.from_uid_property, get_edge_id(self._edge_model), self._dataframe) \
                    .get_updated_dataframe()
        else:
            raise ParseModelFailure(f"illegal value for edge direction: {self._edge_model[mc.DIRECTION]}")

    def _remove_unused_columns(self):
        column_names_to_keep = [self._config.from_uid_property, self._config.to_uid_property]
        self._dataframe = remove_columns(self._dataframe, column_names_to_keep)