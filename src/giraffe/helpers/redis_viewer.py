import redis
import pickle
import itertools
import networkx as nx
from redis import Redis
from typing import List
from waitress import serve
from flask import Flask, request, jsonify, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
r: Redis = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)


def unpickle_into_list(key: str, how_many: int = None):
    if how_many:
        set_content = list((pickle.loads(bytes.fromhex(item)) for item in itertools.islice(r.sscan_iter(name=f'{key}', count=50_000), int(how_many))))
    else:
        set_content = list((pickle.loads(bytes.fromhex(item)) for item in r.sscan_iter(name=f'{key}', count=50_000)))

    return set_content


@app.route('/', methods=['GET'])
def info():
    key_pattern = request.args.get('key_pattern')
    if not key_pattern:
        key_pattern = '*'
    sample_size = request.args.get('sample_size')
    found_keys: List[str] = [key for key in r.keys(pattern=key_pattern)]
    keys_details = []
    for key in found_keys:
        set_content = unpickle_into_list(key=key, how_many=sample_size)
        keys_details.append(
                {
                        'name': key,
                        'type': 'Nodes' if 'nodes' in key else 'Edges',
                        'size': r.scard(name=key),
                        'content': set_content
                }
        )
    response = jsonify(keys_details)
    return response


@app.route('/graph', methods=['GET'])
def graph_from_redis():
    key_pattern: str = request.args.get('key_pattern')
    if not key_pattern:
        key_pattern = '*'

    node_keys = []
    edge_keys = []

    for key in r.keys(pattern=key_pattern):
        if 'nodes' in key:
            node_keys.append(key)
        else:
            edge_keys.append(key)
    if len(node_keys) == 0:
        abort(400, {'message': 'No nodes found !'})

    graph = nx.Graph()

    for node_key in node_keys:
        nodes = unpickle_into_list(key=node_key)
        for node in nodes:
            graph.add_node(node['_uid'], **node)

    for edge_key in edge_keys:
        edges = unpickle_into_list(key=edge_key)
        for edge in edges:
            graph.add_edge(edge['_fromUid'], edge['_toUid'], type=edge_key.split(':')[2])


if __name__ == '__main__':
    serve(app=app)
