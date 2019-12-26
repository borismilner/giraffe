import os

import findspark
from giraffe.exceptions.technical import TechnicalError
from giraffe.helpers import config_helper
from giraffe.helpers.abstract.spark_helper import SparkHelper

findspark.init()
from pyspark.sql import SQLContext, DataFrame
from pyspark.sql import SparkSession


class DevSparkHelper(SparkHelper):
    def __init__(self, config=config_helper.get_config()):
        self.config = config
        self.spark_session = self.get_spark_session()
        self.spark_context = self.spark_session.sparkContext

    @staticmethod
    def list_external_jars(jars_folder: str) -> str:
        if not os.path.isdir(jars_folder):
            raise TechnicalError(f'Path is not a folder: {jars_folder}')
        jar_files = ','.join([f'{os.path.join(jars_folder, file_name)}' for file_name in os.listdir(jars_folder) if file_name.endswith('.jar')])
        return jar_files

    def get_spark_session(self):
        spark = SparkSession.builder.appName(self.config.spark_app_name) \
            .config("spark.jars", DevSparkHelper.list_external_jars(jars_folder=self.config.external_jars_folder)) \
            .config("spark.redis.host", self.config.redis_host_address) \
            .config("spark.redis.port", self.config.redis_port) \
            .getOrCreate()  # Adjust this configuration
        return spark

    def read_df_from_elasticsearch_index(self, index_name: str) -> DataFrame:
        sql_context = SQLContext(self.spark_session.sparkContext)
        es_df = sql_context.read.format("org.elasticsearch.spark.sql").load(f"{index_name}")
        return es_df

    def write_df_to_redis(self, df, key_prefix: str):
        df.write.format("org.apache.spark.sql.redis") \
            .option("table", key_prefix) \
            .option("key.column", self.config.uid_property) \
            .mode('Overwrite') \
            .save()

    # # noinspection PyMethodMayBeStatic
    # def get_string_dict_dataframe(self, df: DataFrame, column_name: str = 'graph_node') -> DataFrame:
    #     ready_for_redis = df.rdd.map(lambda row: SparkHelper.to_dictionary_string(row)).toDF('string').withColumnRenamed('value', column_name)
    #     return ready_for_redis
