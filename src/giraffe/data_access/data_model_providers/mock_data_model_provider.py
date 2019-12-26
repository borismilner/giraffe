from typing import Iterable
from typing import List

from giraffe.data_access.abstract.data_and_model_provider import data_and_graph_model
from giraffe.data_access.abstract.data_and_model_provider import DataAndModelProvider
from giraffe.data_access.abstract.data_and_model_provider import required_data_source
from giraffe.helpers.dev_spark_helper import DevSparkHelper


class MockDataAndModelProvider(DataAndModelProvider):
    def get_data_and_model_for(self, source_descriptions: Iterable[required_data_source]) -> Iterable[data_and_graph_model]:
        spark_helper = DevSparkHelper()
        sc = spark_helper.get_spark_session().sparkContext
        data_and_graph_models: List[data_and_graph_model] = []

        for source in ['source-1', 'source-2']:
            columns = ['_uid', 'name', 'age', 'likes', 'email']
            data = [(f'{source}-{i}', f'{i}-{source}', i, [f'{i}-Oranges', f'{i}-Apples'], f'{i}-{source}@gmail.com') for i in range(0, 10)]
            df_model = data_and_graph_model(source_name=source,
                                            data=sc.parallelize(data).toDF(columns),
                                            graph_model=f'A mock model for source {source}.')
            data_and_graph_models.append(df_model)

        return data_and_graph_models
