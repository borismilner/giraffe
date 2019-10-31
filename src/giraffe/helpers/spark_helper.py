import os
from os import listdir
from os.path import join
from giraffe.helpers.config_helper import ConfigHelper
import findspark
from giraffe.exceptions.technical import TechnicalError

findspark.init()
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


class SparkHelper(object):
    def __init__(self, config: ConfigHelper = ConfigHelper()):
        self.config = config
        self.spark_session = self.get_spark_session()

    @staticmethod
    def list_external_jars(jars_folder: str) -> str:
        if not os.path.isdir(jars_folder):
            raise TechnicalError(f'Path is not a folder: {jars_folder}')
        jar_files = ','.join([f'{join(jars_folder, file_name)}' for file_name in listdir(jars_folder) if file_name.endswith('.jar')])
        return jar_files

    def get_spark_session(self):
        spark = SparkSession.builder.appName(self.config.spark_app_name) \
            .config("spark.jars", SparkHelper.list_external_jars(jars_folder=self.config.external_jars_folder)) \
            .config("spark.redis.host", self.config.redis_host_address) \
            .config("spark.redis.port", self.config.redis_port) \
            .config("es.read.field.as.array.include", "long-text").getOrCreate()  # Adjust this configuration
        return spark

    def read_elasticsearch_index(self, index_name: str):
        sql_context = SQLContext(self.spark_session.sparkContext)
        es_df = sql_context.read.format("org.elasticsearch.spark.sql").load(f"{index_name}")
        return es_df
