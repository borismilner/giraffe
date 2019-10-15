from giraffe.helpers.config_helper import ConfigHelper

config = ConfigHelper()

test_nodes = [
    {
        '_uid': i,
        '_label': config.test_label,
        'age': i ** 2
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
