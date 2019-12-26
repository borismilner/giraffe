from giraffe.exceptions.logical import TransformerError
from pyspark.sql import DataFrame
from typing import List
from pyspark.sql.functions import concat_ws, sha2, array, col

def remove_columns(src_dataframe: DataFrame, column_names_to_keep: List[str]) -> DataFrame:
    all_columns = get_column_names(src_dataframe)
    columns_to_remove = list(set(all_columns) - set(column_names_to_keep))
    updated_dataframe = src_dataframe.drop(*columns_to_remove)
    return updated_dataframe

def rename_column(src_dataframe: DataFrame, existing_name, new_name) -> DataFrame:
    if existing_name not in get_column_names(src_dataframe):
        raise TransformerError(f"failed to rename existing column {existing_name}, it not exist")
    dataframe = src_dataframe.withColumn(new_name, src_dataframe[existing_name])
    return dataframe

def add_column_simple(src_dataframe: DataFrame, key: str, value: str) -> DataFrame:
    dataframe = src_dataframe.withColumn(key, value)
    return dataframe


def get_column_names(dataframe: DataFrame) -> List[str]:
    return list(map(lambda item: item[0], dataframe.dtypes))

def add_concat_columns(src_dataframe: DataFrame, new_column_name:str, delimiter: str, columns: list, hash_column: bool) -> DataFrame:
    concatenated_column = concat_ws(delimiter, *columns)
    concatenated_column = sha2(concatenated_column, 256) if hash_column else concatenated_column
    dataframe = src_dataframe.withColumn(new_column_name, concatenated_column)
    return dataframe

def add_array_column(src_dataframe: DataFrame, columns_to_take: List[str], new_column_name: str) -> DataFrame:
    dataframe = src_dataframe.withColumn(new_column_name, array(columns_to_take))
    return dataframe

def cast_column(src_dataframe: DataFrame, column_name_to_cast: str, required_type, required_type_format = None) -> DataFrame:
    supported_types = ["int", "float"]
    if required_type not in supported_types:
        raise TransformerError(f"can't cast column '{column_name_to_cast}' to '{required_type}', supported types: {supported_types}")
    dataframe = src_dataframe.withColumn(column_name_to_cast, col(column_name_to_cast).cast(required_type))
    return dataframe