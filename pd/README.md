# PD-Lite Design Notes

This package bootstraps NoKV's control-plane work by following TinyKV/TiKV
separation principles:

1. Data plane (`raftstore`) handles replication and apply.
2. Control plane (`pd`) tracks cluster metadata, serves routing info, allocates
   IDs, and provides a global timestamp service.

Current scope in this branch:

1. `pd/core`: in-memory cluster metadata model and route lookup.
2. `pd/core`: global ID allocator primitive.
3. `pd/tso`: monotonic timestamp allocator primitive.

Planned next steps:

1. Add `pdpb` RPC definitions and server endpoints.
2. Add persistent storage backend for PD metadata.
3. Integrate raftstore heartbeat/reporting and client route refresh.

