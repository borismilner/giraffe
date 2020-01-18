import collections
from abc import ABC, abstractmethod
from typing import Iterable

# Receives a description of required data
# Returns a spark-data-frame and a graph-model

required_data_source = collections.namedtuple('required_data_and_model', ['data_and_model_description'])
data_and_graph_model = collections.namedtuple('data_and_graph_model', ['source_name', 'data', 'graph_model'])


# For example, can get the model from inception, and the data as a dataframe from hive/oracle/elastic/....
# In other situations, the data may even not be in the form of a dataframe, and the model may come in a completely different way.

class DataAndModelProvider(ABC):

    @abstractmethod
    def get_data_and_model_for(self, source_descriptions: Iterable[required_data_source]) -> Iterable[data_and_graph_model]:
        pass
