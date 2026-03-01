# PD-lite (`pd/`)

`pd/` is NoKV's distributed control-plane module.

Current role:
- runtime routing source (`GetRegionByKey`)
- store/region heartbeat ingestion
- global allocators (`AllocID`, `Tso`)
- basic scheduling operation downlink (leader transfer)

## Package map

- `core/`: in-memory cluster model + allocator state
- `server/`: gRPC PD service
- `client/`: gRPC client wrapper
- `adapter/`: store-side sink that forwards heartbeats to PD and drains operations
- `storage/`: PD persistence abstraction + local file-backed implementation
- `tso/`: monotonic timestamp allocator

## Runtime note

`nokv serve` is PD-only in distributed mode. Control-plane truth comes from PD.

## Detailed docs

Use the full design/ops doc:

- [`docs/pd.md`](../docs/pd.md)

