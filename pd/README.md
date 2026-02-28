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
4. `pd/server`: gRPC service implementation for heartbeat/route/ID/TSO RPCs.
5. `pd/client`: gRPC client wrapper for store-side integration.
6. `pd/adapter`: `scheduler.RegionSink` bridge that forwards raftstore heartbeats to PD.

Planned next steps:

1. Add persistent storage backend for PD metadata.
2. Integrate raftstore heartbeat/reporting and client route refresh.
3. Add membership/election model for high-availability PD deployment.
