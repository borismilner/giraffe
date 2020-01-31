import decimal
import pickle
from abc import ABC
from abc import abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class SparkHelper(ABC):

    @abstractmethod
    def get_spark_session(self) -> SparkSession:
        pass

    @abstractmethod
    def close_spark_session(self) -> None:
        pass

    @staticmethod
    def get_string_dict_dataframe(df: DataFrame, column_name: str = 'graph_node') -> DataFrame:
        def to_dictionary_string(row):
            row_as_dictionary = {}
            for key, value in zip(row.__fields__, row):
                if isinstance(value.__class__, decimal.Decimal):
                    value = float(value)
                row_as_dictionary[key] = value
            return pickle.dumps(row_as_dictionary).hex()

        ready_for_redis = df.rdd.map(lambda row: to_dictionary_string(row)).toDF('string').withColumnRenamed('value', column_name)
        return ready_for_redis
