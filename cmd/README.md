# Commands (`cmd/`)

`cmd/` contains NoKV executable entrypoints.

## Binaries

- `cmd/nokv`: core operator CLI (`stats`, `manifest`, `vlog`, `regions`, `serve`, `pd`)
- `cmd/nokv-config`: cluster/bootstrap config helpers
- `cmd/nokv-redis`: Redis-compatible gateway (local mode + raft mode)

## Runtime roles

- `nokv serve`: starts a storage node (raftstore + TinyKv RPC) and connects to PD in distributed mode.
- `nokv pd`: starts the PD-lite control-plane service.
- `nokv-redis`: provides Redis protocol access on top of NoKV / raftstore client.

## Detailed docs

- [`docs/cli.md`](../docs/cli.md)
- [`docs/architecture.md`](../docs/architecture.md)
- [`docs/pd.md`](../docs/pd.md)

