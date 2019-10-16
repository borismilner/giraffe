from giraffe.helpers.config_helper import ConfigHelper

config = ConfigHelper()

test_nodes = [
    {
        '_meta': 'OperationBoom.[ingest_node].[officer, gentleman]',
        '_uid': i,
        'name': f'person{i}',
        'age': i,
        'email': f'person{i}@gmail.com'
    }
    for i in range(0, config.number_of_test_nodes)
]

test_edges = [
    {
        '_fromLabel': config.test_label,
        '_fromUid': i,
        '_toLabel': config.test_label,
        '_toUid': i * 2,
        '_edgeType': config.test_edge_type
    }
    for i in range(0, config.number_of_test_edges)
]
