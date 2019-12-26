import giraffe.configuration.common_testing_artifactrs as commons
import pytest
from giraffe.data_access import neo_db


@pytest.fixture(autouse=True)
def run_around_tests():
    commons.init_neo_test_data()
    yield
    commons.purge_neo4j_database()


def test_create_index_if_not_exists(neo, config_helper):
    db: neo_db.NeoDB = neo
    db.drop_index_if_exists(label=config_helper.test_labels[0], property_name=config_helper.test_property)
    summary = db.create_index_if_not_exists(label=config_helper.test_labels[0], property_name=config_helper.test_property)
    assert summary.counters.indexes_added == 1
    summary = db.drop_index_if_exists(label=config_helper.test_labels[0], property_name=config_helper.test_property)
    assert summary.counters.indexes_removed == 1


def test_drop_index_if_exists(neo, config_helper):
    db: neo_db.NeoDB = neo
    db.drop_index_if_exists(label=config_helper.test_labels[0], property_name=config_helper.test_property)
    summary = db.drop_index_if_exists(label=config_helper.test_labels[0], property_name=config_helper.test_property)
    assert summary is None
    db.create_index_if_not_exists(label=config_helper.test_labels[0], property_name=config_helper.test_property)
    summary = db.drop_index_if_exists(label=config_helper.test_labels[0], property_name=config_helper.test_property)
    assert summary.counters.indexes_removed == 1


def test_merge_nodes(neo, config_helper, nodes, edges):
    db: neo_db.NeoDB = neo
    commons.purge_neo4j_database()
    summary = db.merge_nodes(nodes=nodes, label=config_helper.test_labels[0])
    assert summary.counters.nodes_created == len(nodes)


def test_merge_edges(neo, config_helper, edges):
    db: neo_db.NeoDB = neo
    summary = db.merge_edges(edges=edges, from_label=config_helper.test_labels[0], to_label=config_helper.test_labels[0])
    assert summary.counters.relationships_created == config_helper.number_of_test_edges


def test_delete_nodes_by_property(neo, config_helper):
    db: neo_db.NeoDB = neo
    test_label = config_helper.test_labels[0]

    query = f"MATCH(n:{test_label}) return count(n) as count"
    before_deletion_count = db.pull_query(query=query).value()[0]
    # result: dict = db.delete_nodes_by_property(label=test_label, property_name='name', property_value='Person0')
    result: dict = db.delete_nodes_by_properties(label=test_label, property_name_value_tuples=[('name', 'Person0')])
    after_deletion_count = db.pull_query(query=query).value()[0]
    deleted_count = result['total']
    assert deleted_count > 0
    assert before_deletion_count - after_deletion_count == deleted_count
