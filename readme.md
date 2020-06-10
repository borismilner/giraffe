![alt text](resources/images/giraffe_page.png "Giraffe!")  
# Giraffe - Graph With Pleasure  
  
## Features  
- [x] Neo4j Integration  
- [x] Redis Integration  
- [x] Progress Monitoring
- [x] Unit/Integration Tests

## Tested
- Python 3.6, 3.7, 3.8
- Neo4j 3.5.12 community edition (Linux/Windows)
- Redis 5.0.3 (Linux)

## Things To Know
1. **REDIS Key/Value Structure**
    - job_name:operation:arguments
        - Example: AwesomeJob:nodes_ingest:MyPrimaryLabel
        - Example: AwesomeJob:edges_ingest:EDGE_TYPE,LabelOfFromNode,LabelOfToNode  
    - node_body_format: {'**_uid**': 123, ... } - _uid is mandatory and must have globally unique value.
        - Example: {'_uid': 790, 'name': 'person790', 'age': 790, 'email': 'person790@gmail.com'}
    - edge_body_format: {'**_uid**': _123_, ... } - _uid is mandatory and must have globally unique value.
        - Example: {'_fromUid': 331, '_toUid': 662, '_edgeType': 'EDGE_TYPE'} - all are mandatory.
    - note that both **nodes and edges bodies** are string representation of **python dictionary** (it is required).
1. **Supported Operations**
    - nodes_ingest
    - edges_ingest    
1. **Configuration Format**
    - https://wiki.python.org/moin/ConfigParserExamples  
## Current Limitations

1. In **node-ingestion**, since UNWIND won't allow dynamic labels (as it is being compiled into the query)  
all nodes in a batch must have the same label.
1. In **edge-ingestion**, while it is possible to match source and target nodes by their _uid (without their labels)  
it is too slow and thus it currently is mandatory to provide both...
