import collections
from abc import ABC, abstractmethod
from typing import Dict
from typing import List

from pyspark.sql import DataFrame

translation_request = collections.namedtuple('translation_request', 'translation_request')


class DataToGraphEntitiesProvider(ABC):

    # Return value of translate function is currently a mapping between a redis-key to list of dataframes to write into it.
    # It may change if we decide that the translation returns "ready to use" strings.
    @abstractmethod
    def translate(self, request: translation_request) -> Dict[str, List[DataFrame]]:
        pass

    # @abstractmethod
    # def translate(self, src_df: DataFrame, model: dict, streaming_id: str) -> Dict[str, List[DataFrame]]:
    #     pass
