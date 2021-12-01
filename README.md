# telekinesis_data

Hierarchical, distributed, versioned and metadata queryable database - that leverages telekinesis

## Features/Roadmap

- [x] Hierarchical
  - [x] Data is stored in a tree structure
  - [x] Descendants of a node can be easily queried
  - [x] It is easy to obtain a handle for any node, having access to all descendants but no parents or siblings
- [ ] Metadata
  - [x] Nodes contain arbitrary user metadata
  - [ ] Nodes can be queried based on the metadata
- [x] Versioned
  - [x] Data can be stored in different branches
  - [x] Any past state can be easily recreated/queried
  - [x] A branch can be created based on another at some old point in time
- [x] Distributed
- [ ] Backed-up to S3
