from logging import Logger
from typing import List

import pytest
from giraffe.business_logic import ingestion_manger
from giraffe.translation.translator import Translator
from pyspark.sql import DataFrame


@pytest.mark.skip(reason="Test is currently too long")
def test_integration_without_timing(data_frame_and_models, logger: Logger, spark_helper, config_helper, redis_db):
    logger.info("Initiating translator.")
    graph_entities_factory = Translator()

    test_sets_to_process = ['dummy', 'other_dummy']

    translations: List[dict] = []
    for test_set in test_sets_to_process:
        model_table_tuple = data_frame_and_models[test_set]

        streaming_id = test_set
        model = model_table_tuple[0]
        data_frame = model_table_tuple[1]

        translation_result = graph_entities_factory.translate(src_df=data_frame,
                                                              model=model,
                                                              streaming_id=streaming_id)

        # TODO: Add assertions per test-set

        translations.append(translation_result)

        for translation in translations:
            for prefix in translation.keys():
                data_frames: List[DataFrame] = translation[prefix]
                logger.debug(f'Writing into redis the prefix: {prefix}')
                for df in data_frames:
                    string_column_name = 'graph_node'
                    ready_for_redis = spark_helper.get_string_dict_dataframe(df=df, column_name=string_column_name)
                    ready_for_redis.write \
                        .option('redis_host', config_helper.redis_host_address) \
                        .option('redis_port', '6379') \
                        .option('redis_column_name', string_column_name) \
                        .option('redis_set_key', prefix) \
                        .format(source='spark.to.redis') \
                        .save()

            logger.info('Finished writing to redis')

        im = ingestion_manger.IngestionManager(config_helper=config_helper)
        im.process_redis_content(key_prefix=streaming_id, request_id='unit-testing')
        r = redis_db.get_driver()
        # r.flushall()

    logger.info('Done.')
