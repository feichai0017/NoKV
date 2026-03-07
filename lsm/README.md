# LSM (`lsm/`)

`lsm/` is NoKV's on-disk storage core for the embedded engine.

Main responsibilities:
- write path plumbing (memtable -> flush -> SST)
- read path over memtable/levels/tables/iterators
- compaction, ingest, and level management
- table/index/filter/cache integration

## Package map

- `lsm.go`, `executor.go`: top-level engine orchestration and read/write execution
- `memtable.go`: mutable in-memory write buffer
- `levels.go`: level manager, table placement, compaction planning helpers
- `table.go`, `builder.go`, `iterator.go`: SST format, block/table build, table iteration
- `ingest.go`: ingest fast path / buffered ingest logic
- `compact/`: compaction policy and execution helpers
- `flush/`: memtable flush stage machine

## Detailed docs

- [`docs/architecture.md`](../docs/architecture.md)
- [`docs/compaction.md`](../docs/compaction.md)
- [`docs/flush.md`](../docs/flush.md)
- [`docs/memtable.md`](../docs/memtable.md)

