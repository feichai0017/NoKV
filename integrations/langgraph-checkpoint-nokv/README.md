# langgraph-checkpoint-nokv

Experimental LangGraph checkpoint integration for NoKV.

Current scope:

- thin Python fsmeta client wrapper;
- content-addressed local file body store;
- fsmeta layout helpers for checkpoints, channel blobs, writes, and logical
  thread tombstones;
- typed checkpoint body references that can be stored in fsmeta
  `opaque_attrs`.
- `NoKVCheckpointSaver` with LangGraph checkpoint, pending write, logical
  delete, and delta channel history support;
- benchmark harness for delta channel fast-path comparisons.

Engineering notes:

- [DeltaChannel optimization handoff](docs/delta-channel-optimization-handoff.md)
