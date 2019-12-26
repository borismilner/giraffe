from giraffe.transformation.model_utilities import validate_array_property
from pyspark.sql.functions import lit
import giraffe.transformation.model_constants as mc
from giraffe.exceptions.logical import TransformerError
from giraffe.transformation.spark_utilities import rename_column, add_column_simple, add_concat_columns, \
    add_array_column, cast_column
from giraffe.helpers.config_helper import ConfigHelper


class Property:
    def __init__(self, key, property_model, dataframe_to_update):
        self._dataframe_to_update = dataframe_to_update
        self._property_model = property_model
        self._key = key
        self._update_function = None
        self._config = ConfigHelper()
        self._type = None

    def get_updated_dataframe(self):
        self._set_update_function()
        updated_dataframe = self._update_function()
        return updated_dataframe

    def _set_update_function(self):
        self._check_if_typed_property()
        if isinstance(self._property_model, str) and self._property_model.startswith(mc.PROPERTY_FROM_TABLE_PREFIX):
            self._update_function = self._rename_update
        if isinstance(self._property_model, dict):
            if mc.FIELD in self._property_model:
                if mc.INCLUDE in self._property_model:
                    self._update_function = self._field_and_include_update
            elif mc.FIELDS in self._property_model and isinstance(self._property_model[mc.FIELDS], list) and len(self._property_model[mc.FIELDS] > 0):
                self._update_function = self._fields_delimiter_update
        if isinstance(self._property_model, list):
            self._update_function = self._array_property_update

        if self._update_function is None:
            raise TransformerError(f"property '{self._key}': Unsupported property structure: {self._property_model}")

    def _check_if_typed_property(self):
        if isinstance(self._property_model, dict) and mc.PRIMITIVE_TYPE in self._property_model:
            self._cast_required = True
            propert_format = self._property_model.get(mc.PRIMITIVE_TYPE_FORMAT)
            self._type = {
                mc.PRIMITIVE_TYPE: self._property_model[mc.PRIMITIVE_TYPE],
                mc.PRIMITIVE_TYPE_FORMAT: propert_format
            }
            self._property_model = self._property_model[mc.FIELD]
        else:
            self._cast_required = False
            
    def _rename_update(self):
        existing_column_name = self._property_model[1:]
        data_frame = rename_column(self._dataframe_to_update, existing_column_name, self._key)
        if self._cast_required:
            data_frame = cast_column(self._dataframe_to_update, self._key, self._type[mc.PRIMITIVE_TYPE], self._type[mc.PRIMITIVE_TYPE_FORMAT])

    def _field_and_include_update(self):
        property_value = self._property_model[mc.INCLUDE]
        return add_column_simple(self._dataframe_to_update, self._key, property_value)

    def _fields_delimiter_update(self):
        fields = self._property_model["fields"]
        columns_to_concat = []
        for field in fields:
            if isinstance(field, str):
                if field.startswith(mc.PROPERTY_FROM_TABLE_PREFIX):
                    columns_to_concat.append(self._dataframe_to_update[field[1:]])
                else:
                    columns_to_concat.append(lit(field))
            elif isinstance(field, dict) and mc.TYPE in field and mc.FIELD in field and field[mc.FIELD].startswith(mc.PROPERTY_FROM_TABLE_PREFIX):
                columns_to_concat.append(self._dataframe_to_update[field[mc.FIELD][1:]])
            else:
                raise TransformerError(f"property '{self._key}': Unsupported property structure: {self._property_model}")
        delimiter = self._property_model[mc.DELIMITER]
        return add_concat_columns(self._dataframe_to_update, self._key, delimiter, columns_to_concat, self._config.hash_uid_column)

    def _array_property_update(self):
        validate_array_property(self._property_model)
        return add_array_column(self._dataframe_to_update, self._property_model, self._key)
