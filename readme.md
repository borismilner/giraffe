![alt text](resources/images/giraffe_page.png "Giraffe!")  
# Giraffe - Graph With Pleasure  
  
## Features  
- [x] Neo4j Integration  
- [x] Redis Integration  
- [x] Unit Testing  
  
## Things To Take Care Of  
- [ ] Improve multi-processing

## Things To Know
1. **REDIS Key Structure**
    - job_name:operation:arguments
        - Example: AwesomeJob:nodes_ingest:MyPrimaryLabel
        - Example: AwesomeJob:edges_ingest:EDGE_TYPE,LabelOfFromNode,LabelOfToNode  
1. **Supported Operations**
    - nodes_ingest
    - edges_ingest    
1. **Configuration Format**
    - https://wiki.python.org/moin/ConfigParserExamples  
## Current Limitations

1. In **node-ingestion**, since UNWIND won't allow dynamic labels (as it is being compiled into the query)  
all nodes in a batch must have the same label.
1. In **edge-ingestion**, while it is possible to match source and target nodes by their _uid (without their labels)  
it is too slow and thus it currently is mandatory to provide both.