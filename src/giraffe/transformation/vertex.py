from giraffe.transformation.spark_utilities import remove_columns, get_column_names
from pyspark.sql import DataFrame
import giraffe.transformation.model_constants as mc
from giraffe.transformation.model_utilities import validate_has_attribute, validate_vertex_properties
from giraffe.transformation.property import Property
from giraffe.helpers import config_helper


class Vertex:
    def __init__(self, vertex_model: dict, src_dataframe: DataFrame) -> DataFrame:
        self._vertex_model = vertex_model,
        self._src_dataframe = src_dataframe
        self._dataframe = None
        self._config = config_helper.get_config()

    def compute_dataframe(self):
        if self._dataframe is not None:
            return self._dataframe
        self._add_properties()
        self._add_uid()
        self._remove_unused_columns()
        return self._dataframe

    def _add_properties(self):
        validate_vertex_properties(self._vertex_model)
        for property_key, property_model in self._vertex_model[mc.PROPERTIES].items:
            self._dataframe = Property(property_key, property_model, self._dataframe)\
                .get_updated_dataframe()

    def _add_uid(self):
        validate_has_attribute(self._vertex_model, mc.ID, mc.VERTEX)
        self._dataframe = \
            Property(self._config.uid_property, self._vertex_model[mc.ID], self._dataframe)\
                .get_updated_dataframe()

    def _remove_unused_columns(self):
        column_names_to_keep = [property_key for property_key in self._vertex_model]
        self._dataframe = remove_columns(self._dataframe, column_names_to_keep)
