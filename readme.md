![alt text](resources/images/giraffe_page.png "Giraffe!")  
# Giraffe - Graph With Pleasure  
  
## Features  
- [x] Neo4j Integration  
- [x] Redis Integration  
- [x] Unit Testing  
  
## Things to take care of  
- [ ] Improve multi-processing

## Things to know
1. **REDIS Key Structure**: job_name:operation:arguments
    - For Example:
        - AwesomeJob:nodes_ingest:MyPrimaryLabel
        - AwesomeJob:edges_ingest:EDGE_TYPE,LabelOfFromNode,LabelOfToNode


2. **Supported Operations**: nodes_ingest, edges_ingest

3. **Configuration Format**: https://wiki.python.org/moin/ConfigParserExamples