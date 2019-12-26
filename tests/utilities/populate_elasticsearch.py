from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers

if __name__ == '__main__':
    es = Elasticsearch()
    actions = [
            {
                    "_index": "test_index",
                    "_id": i,
                    "_source": {
                            "pid": i,
                            "name": f"Name_{i}",
                            "age": i,
                            "org_name": f"Org_{i}",
                            "org_location": f"location_{i}",
                    }
            }
            for i in range(0, 100)
    ]

    helpers.bulk(es, actions)
