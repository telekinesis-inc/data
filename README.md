# telekinesis_data

Hierarchical, distributed, versioned and metadata queryable database - that leverages telekinesis

## Features/Roadmap

- [ ] Hierarchical
  - [x] Data is stored in a tree structure
  - [ ] Descendants of a node can be easily queried
  - [ ] It is easy to obtain a handle for any node, having access to all descendants but no parents or siblings
- [ ] Metadata
  - [x] Nodes contain arbitrary user metadata
  - [ ] Nodes can be queried based on the metadata
- [ ] Versioned
  - [x] Data can be stored in different branches
  - [ ] Any past state can be easily recreated/queried
  - [ ] A branch can be created based on another's time specific snapshot
- [ ] Distributed
- [ ] Backed-up to S3
