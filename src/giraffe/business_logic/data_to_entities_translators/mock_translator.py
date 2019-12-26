from typing import Dict
from typing import List

from giraffe.business_logic.abstract.data_to_graph_translation_provider import DataToGraphEntitiesProvider
from giraffe.business_logic.abstract.data_to_graph_translation_provider import translation_request
from pyspark.sql import DataFrame


class MockDataToGraphEntitiesProvider(DataToGraphEntitiesProvider):
    def translate(self, request: translation_request) -> Dict[str, List[DataFrame]]:
        return {f'{request.translation_request["streaming_id"]}:nodes_ingest:MockPerson': [request.translation_request['src_df']]}
