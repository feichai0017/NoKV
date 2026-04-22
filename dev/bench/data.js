window.BENCHMARK_DATA = {
  "lastUpdate": 1776853259577,
  "repoUrl": "https://github.com/feichai0017/NoKV",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "songguocheng348@gmail.com",
            "name": "eric_song",
            "username": "feichai0017"
          },
          "committer": {
            "email": "songguocheng348@gmail.com",
            "name": "eric_song",
            "username": "feichai0017"
          },
          "distinct": true,
          "id": "5452972cf728504edb8641e0fb6538c4d6585137",
          "message": "feat(config): meta_root.peers — single source of truth for cluster addresses\n\nPreviously meta-root peer addresses lived in two places: the raft_config\nfile had everything EXCEPT the meta-root section, and docker-compose (or\nsystemd units) had to duplicate the addresses as hardcoded --peer /\n--root-peer / --transport-addr flags on every service. That was a real bug\n— changing a meta-root address meant editing both the config file AND the\norchestration file.\n\nAdd meta_root.peers to the config schema so address directory lives in one\nplace:\n\n- config.File.MetaRoot.Peers[].NodeID / Addr / DockerAddr /\n  TransportAddr / DockerTransportAddr / WorkDir / DockerWorkDir\n- Resolvers: MetaRootServicePeers(scope), MetaRootTransportPeers(scope),\n  ResolveMetaRootPeer, ResolveMetaRootServiceAddr,\n  ResolveMetaRootTransportAddr, ResolveMetaRootWorkDir\n- Validation: exactly 3 peers, non-zero node_id, required addresses\n- Sorted by NodeID for deterministic iteration\n\nWire the CLIs to resolve from --config when flags are absent:\n\n- nokv meta-root --config <file> --node-id N:\n  meta_root.peers[N-1] provides --addr, --transport-addr, --workdir\n  meta_root.peers[*] provides the 3 --peer entries\n\n- nokv coordinator --config <file>:\n  coordinator.addr (resolved) provides --addr\n  meta_root.peers[*] provides the 3 --root-peer entries (gRPC addrs, not\n  transport addrs — coord dials meta-root's service port)\n\nBoth still honor explicit CLI flags as overrides (CLI beats config).\n\nUpdate the shipped config / orchestration to exploit this:\n\n- raft_config.example.json: add full meta_root section for host + docker\n  scopes (2380 gRPC / 2480 docker-transport / 3380 host-transport)\n- docker-compose.yml: every meta-root / coordinator service now passes\n  --config=/etc/nokv/raft_config.json + --scope=docker + --node-id\n  (for meta-root). All hardcoded --peer / --root-peer / --transport-addr /\n  --workdir flags are gone. Mount the config file read-only into each\n  container.\n- scripts/ops/serve-meta-root.sh + serve-coordinator.sh: accept\n  --config / --scope and forward them. Legacy flag-driven path still works\n  for deployments without a config file.\n\nDocument the two-layer model clearly in docs/config.md:\n\n  Address layer    : meta_root / coordinator / stores — THIS FILE IS TRUTH,\n                     read on every CLI invocation.\n  Bootstrap seed   : regions — only consumed by bootstrap.sh on first run,\n                     ignored afterwards. Meta-root owns runtime topology.\n\nLifecycles are different: addresses must stay in sync with deployed\ncontainers; regions is a one-shot seed. Editing regions after first\nbootstrap is a no-op for the running cluster (use nokv-config / ccc-audit to\ninspect rooted truth).\n\nVerified: docker compose up -d with new --config commands brings the full\ncluster up, lease elected, 10/10 redis writes succeed. All package tests +\ncmd/nokv tests + coordinator integration tests green. make lint / fmt clean.\n\nCo-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-04-22T20:15:53+10:00",
          "tree_id": "1876ccace38dfdafc8638f72ef85e8c8a54cc3ce",
          "url": "https://github.com/feichai0017/NoKV/commit/5452972cf728504edb8641e0fb6538c4d6585137"
        },
        "date": 1776853254281,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV)",
            "value": 4661,
            "unit": "ns/op\t   6.87 MB/s\t     424 B/op\t      13 allocs/op",
            "extra": "25992 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 4661,
            "unit": "ns/op",
            "extra": "25992 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 6.87,
            "unit": "MB/s",
            "extra": "25992 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 424,
            "unit": "B/op",
            "extra": "25992 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "25992 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV)",
            "value": 12501,
            "unit": "ns/op\t 327.66 MB/s\t     400 B/op\t      20 allocs/op",
            "extra": "8431 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 12501,
            "unit": "ns/op",
            "extra": "8431 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 327.66,
            "unit": "MB/s",
            "extra": "8431 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 400,
            "unit": "B/op",
            "extra": "8431 times\n4 procs"
          },
          {
            "name": "BenchmarkDBSetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 20,
            "unit": "allocs/op",
            "extra": "8431 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV)",
            "value": 1539,
            "unit": "ns/op\t  41.58 MB/s\t     264 B/op\t       5 allocs/op",
            "extra": "93277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - ns/op",
            "value": 1539,
            "unit": "ns/op",
            "extra": "93277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - MB/s",
            "value": 41.58,
            "unit": "MB/s",
            "extra": "93277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - B/op",
            "value": 264,
            "unit": "B/op",
            "extra": "93277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetSmall (github.com/feichai0017/NoKV) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "93277 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV)",
            "value": 4778,
            "unit": "ns/op\t 857.22 MB/s\t    9160 B/op\t       7 allocs/op",
            "extra": "24258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - ns/op",
            "value": 4778,
            "unit": "ns/op",
            "extra": "24258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - MB/s",
            "value": 857.22,
            "unit": "MB/s",
            "extra": "24258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - B/op",
            "value": 9160,
            "unit": "B/op",
            "extra": "24258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBGetLarge (github.com/feichai0017/NoKV) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "24258 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV)",
            "value": 174520,
            "unit": "ns/op\t  93.88 MB/s\t  193392 B/op\t     147 allocs/op",
            "extra": "1370 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - ns/op",
            "value": 174520,
            "unit": "ns/op",
            "extra": "1370 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - MB/s",
            "value": 93.88,
            "unit": "MB/s",
            "extra": "1370 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - B/op",
            "value": 193392,
            "unit": "B/op",
            "extra": "1370 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/NoSync (github.com/feichai0017/NoKV) - allocs/op",
            "value": 147,
            "unit": "allocs/op",
            "extra": "1370 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV)",
            "value": 1767964,
            "unit": "ns/op\t   9.27 MB/s\t   46713 B/op\t     148 allocs/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - ns/op",
            "value": 1767964,
            "unit": "ns/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - MB/s",
            "value": 9.27,
            "unit": "MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - B/op",
            "value": 46713,
            "unit": "B/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncInline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 148,
            "unit": "allocs/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV)",
            "value": 545920,
            "unit": "ns/op\t  30.01 MB/s\t   46740 B/op\t     149 allocs/op",
            "extra": "199 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - ns/op",
            "value": 545920,
            "unit": "ns/op",
            "extra": "199 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - MB/s",
            "value": 30.01,
            "unit": "MB/s",
            "extra": "199 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - B/op",
            "value": 46740,
            "unit": "B/op",
            "extra": "199 times\n4 procs"
          },
          {
            "name": "BenchmarkDBBatchSet/SyncPipeline (github.com/feichai0017/NoKV) - allocs/op",
            "value": 149,
            "unit": "allocs/op",
            "extra": "199 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV)",
            "value": 1731802,
            "unit": "ns/op\t       8 B/op\t       0 allocs/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - ns/op",
            "value": 1731802,
            "unit": "ns/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorScan (github.com/feichai0017/NoKV) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "69 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV)",
            "value": 245.1,
            "unit": "ns/op\t      32 B/op\t       1 allocs/op",
            "extra": "485299 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - ns/op",
            "value": 245.1,
            "unit": "ns/op",
            "extra": "485299 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - B/op",
            "value": 32,
            "unit": "B/op",
            "extra": "485299 times\n4 procs"
          },
          {
            "name": "BenchmarkDBIteratorSeek (github.com/feichai0017/NoKV) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "485299 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 485.7,
            "unit": "ns/op\t 131.78 MB/s\t    1541 B/op\t       0 allocs/op",
            "extra": "207218 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 485.7,
            "unit": "ns/op",
            "extra": "207218 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 131.78,
            "unit": "MB/s",
            "extra": "207218 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 1541,
            "unit": "B/op",
            "extra": "207218 times\n4 procs"
          },
          {
            "name": "BenchmarkARTInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "207218 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 151.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "779857 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 151.1,
            "unit": "ns/op",
            "extra": "779857 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "779857 times\n4 procs"
          },
          {
            "name": "BenchmarkARTGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "779857 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 150.3,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "895413 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 150.3,
            "unit": "ns/op",
            "extra": "895413 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "895413 times\n4 procs"
          },
          {
            "name": "BenchmarkARTSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "895413 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 61.72,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1641770 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 61.72,
            "unit": "ns/op",
            "extra": "1641770 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1641770 times\n4 procs"
          },
          {
            "name": "BenchmarkARTIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1641770 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index)",
            "value": 1139,
            "unit": "ns/op\t  56.17 MB/s\t     159 B/op\t       1 allocs/op",
            "extra": "164398 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 1139,
            "unit": "ns/op",
            "extra": "164398 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 56.17,
            "unit": "MB/s",
            "extra": "164398 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 159,
            "unit": "B/op",
            "extra": "164398 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsert (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "164398 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index)",
            "value": 465.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "231871 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 465.6,
            "unit": "ns/op",
            "extra": "231871 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "231871 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistGet (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "231871 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index)",
            "value": 442.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "287079 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 442.6,
            "unit": "ns/op",
            "extra": "287079 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "287079 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistSeek (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "287079 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index)",
            "value": 44.6,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "2652882 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 44.6,
            "unit": "ns/op",
            "extra": "2652882 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "2652882 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistIteratorNext (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "2652882 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index)",
            "value": 385.7,
            "unit": "ns/op\t 165.94 MB/s\t     161 B/op\t       1 allocs/op",
            "extra": "322881 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 385.7,
            "unit": "ns/op",
            "extra": "322881 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 165.94,
            "unit": "MB/s",
            "extra": "322881 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 161,
            "unit": "B/op",
            "extra": "322881 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertSequential (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "322881 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index)",
            "value": 973.4,
            "unit": "ns/op\t  65.75 MB/s\t     162 B/op\t       1 allocs/op",
            "extra": "387175 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - ns/op",
            "value": 973.4,
            "unit": "ns/op",
            "extra": "387175 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - MB/s",
            "value": 65.75,
            "unit": "MB/s",
            "extra": "387175 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - B/op",
            "value": 162,
            "unit": "B/op",
            "extra": "387175 times\n4 procs"
          },
          {
            "name": "BenchmarkSkiplistInsertRandom (github.com/feichai0017/NoKV/engine/index) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "387175 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 26984,
            "unit": "ns/op\t 303.59 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "4832 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 26984,
            "unit": "ns/op",
            "extra": "4832 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 303.59,
            "unit": "MB/s",
            "extra": "4832 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "4832 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "4832 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 37516,
            "unit": "ns/op\t 218.36 MB/s\t   34644 B/op\t     210 allocs/op",
            "extra": "2766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 37516,
            "unit": "ns/op",
            "extra": "2766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - MB/s",
            "value": 218.36,
            "unit": "MB/s",
            "extra": "2766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 34644,
            "unit": "B/op",
            "extra": "2766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMSetBatch/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 210,
            "unit": "allocs/op",
            "extra": "2766 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 6462025,
            "unit": "ns/op\t71593384 B/op\t     476 allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 6462025,
            "unit": "ns/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 71593384,
            "unit": "B/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 476,
            "unit": "allocs/op",
            "extra": "18 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 7923914,
            "unit": "ns/op\t71593453 B/op\t     473 allocs/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 7923914,
            "unit": "ns/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 71593453,
            "unit": "B/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMRotateFlush/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 473,
            "unit": "allocs/op",
            "extra": "19 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 328.4,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "316390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 328.4,
            "unit": "ns/op",
            "extra": "316390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "316390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "316390 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 556.9,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "185660 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 556.9,
            "unit": "ns/op",
            "extra": "185660 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "185660 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMGetMemtableHit/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "185660 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 156.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "725208 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 156.2,
            "unit": "ns/op",
            "extra": "725208 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "725208 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/art (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "725208 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 353,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "345771 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 353,
            "unit": "ns/op",
            "extra": "345771 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "345771 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMemtableIterSeek/skiplist (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "345771 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 34777,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "3343 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 34777,
            "unit": "ns/op",
            "extra": "3343 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "3343 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "3343 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 90.12,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "1331314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 90.12,
            "unit": "ns/op",
            "extra": "1331314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "1331314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "1331314 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 17899,
            "unit": "ns/op\t     240 B/op\t       5 allocs/op",
            "extra": "6679 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 17899,
            "unit": "ns/op",
            "extra": "6679 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 240,
            "unit": "B/op",
            "extra": "6679 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "6679 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 504.5,
            "unit": "ns/op\t     232 B/op\t       4 allocs/op",
            "extra": "209709 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 504.5,
            "unit": "ns/op",
            "extra": "209709 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "209709 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointHitPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 4,
            "unit": "allocs/op",
            "extra": "209709 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 17234,
            "unit": "ns/op\t       8 B/op\t       1 allocs/op",
            "extra": "6574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 17234,
            "unit": "ns/op",
            "extra": "6574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 8,
            "unit": "B/op",
            "extra": "6574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 1,
            "unit": "allocs/op",
            "extra": "6574 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 475.3,
            "unit": "ns/op\t     200 B/op\t       3 allocs/op",
            "extra": "222753 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 475.3,
            "unit": "ns/op",
            "extra": "222753 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 200,
            "unit": "B/op",
            "extra": "222753 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelPointInRangeMissPruning/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "222753 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 64366,
            "unit": "ns/op\t   19032 B/op\t      11 allocs/op",
            "extra": "1995 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 64366,
            "unit": "ns/op",
            "extra": "1995 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 19032,
            "unit": "B/op",
            "extra": "1995 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "1995 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 905.5,
            "unit": "ns/op\t     608 B/op\t      11 allocs/op",
            "extra": "133018 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 905.5,
            "unit": "ns/op",
            "extra": "133018 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 608,
            "unit": "B/op",
            "extra": "133018 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_1/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 11,
            "unit": "allocs/op",
            "extra": "133018 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 61039,
            "unit": "ns/op\t   22064 B/op\t      54 allocs/op",
            "extra": "1881 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 61039,
            "unit": "ns/op",
            "extra": "1881 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 22064,
            "unit": "B/op",
            "extra": "1881 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "1881 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 4656,
            "unit": "ns/op\t    3696 B/op\t      54 allocs/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 4656,
            "unit": "ns/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 3696,
            "unit": "B/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_8/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 54,
            "unit": "allocs/op",
            "extra": "25118 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 92129,
            "unit": "ns/op\t   45809 B/op\t     390 allocs/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 92129,
            "unit": "ns/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 45809,
            "unit": "B/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "1192 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 35050,
            "unit": "ns/op\t   27888 B/op\t     390 allocs/op",
            "extra": "3019 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 35050,
            "unit": "ns/op",
            "extra": "3019 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 27888,
            "unit": "B/op",
            "extra": "3019 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelIteratorBoundsPruning/width_64/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 390,
            "unit": "allocs/op",
            "extra": "3019 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1929,
            "unit": "ns/op\t    1544 B/op\t      21 allocs/op",
            "extra": "53896 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1929,
            "unit": "ns/op",
            "extra": "53896 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1544,
            "unit": "B/op",
            "extra": "53896 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 21,
            "unit": "allocs/op",
            "extra": "53896 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 1751,
            "unit": "ns/op\t    1152 B/op\t      16 allocs/op",
            "extra": "66288 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 1751,
            "unit": "ns/op",
            "extra": "66288 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1152,
            "unit": "B/op",
            "extra": "66288 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_8/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 16,
            "unit": "allocs/op",
            "extra": "66288 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 10130,
            "unit": "ns/op\t    7144 B/op\t     105 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 10130,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 7144,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 105,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 10915,
            "unit": "ns/op\t    6752 B/op\t     100 allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 10915,
            "unit": "ns/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 6752,
            "unit": "B/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_64/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 100,
            "unit": "allocs/op",
            "extra": "10000 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 38580,
            "unit": "ns/op\t   26344 B/op\t     393 allocs/op",
            "extra": "3199 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 38580,
            "unit": "ns/op",
            "extra": "3199 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 26344,
            "unit": "B/op",
            "extra": "3199 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/manual_seek_break (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 393,
            "unit": "allocs/op",
            "extra": "3199 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 42006,
            "unit": "ns/op\t   25952 B/op\t     388 allocs/op",
            "extra": "2996 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 42006,
            "unit": "ns/op",
            "extra": "2996 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 25952,
            "unit": "B/op",
            "extra": "2996 times\n4 procs"
          },
          {
            "name": "BenchmarkTableIteratorBlockBounds/width_256/block_range (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 388,
            "unit": "allocs/op",
            "extra": "2996 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 46096,
            "unit": "ns/op\t     296 B/op\t       9 allocs/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 46096,
            "unit": "ns/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 296,
            "unit": "B/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "2505 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 984.7,
            "unit": "ns/op\t     272 B/op\t       6 allocs/op",
            "extra": "114084 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 984.7,
            "unit": "ns/op",
            "extra": "114084 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "114084 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/deep_hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 6,
            "unit": "allocs/op",
            "extra": "114084 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 53732,
            "unit": "ns/op\t      64 B/op\t       5 allocs/op",
            "extra": "2136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 53732,
            "unit": "ns/op",
            "extra": "2136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 64,
            "unit": "B/op",
            "extra": "2136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "2136 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 520,
            "unit": "ns/op\t      40 B/op\t       2 allocs/op",
            "extra": "225256 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 520,
            "unit": "ns/op",
            "extra": "225256 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 40,
            "unit": "B/op",
            "extra": "225256 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelPointPruning/miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "225256 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 22633,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5504 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 22633,
            "unit": "ns/op",
            "extra": "5504 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5504 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5504 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 22256,
            "unit": "ns/op\t    4704 B/op\t      13 allocs/op",
            "extra": "5538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 22256,
            "unit": "ns/op",
            "extra": "5538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4704,
            "unit": "B/op",
            "extra": "5538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/hit/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 13,
            "unit": "allocs/op",
            "extra": "5538 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 21106,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5900 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 21106,
            "unit": "ns/op",
            "extra": "5900 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5900 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5900 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 21207,
            "unit": "ns/op\t    4472 B/op\t       9 allocs/op",
            "extra": "5930 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 21207,
            "unit": "ns/op",
            "extra": "5930 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 4472,
            "unit": "B/op",
            "extra": "5930 times\n4 procs"
          },
          {
            "name": "BenchmarkLevelL0OverlapFallback/in_range_miss/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "5930 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 31469,
            "unit": "ns/op\t     232 B/op\t       7 allocs/op",
            "extra": "3614 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 31469,
            "unit": "ns/op",
            "extra": "3614 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 232,
            "unit": "B/op",
            "extra": "3614 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3614 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 818,
            "unit": "ns/op\t     214 B/op\t       5 allocs/op",
            "extra": "136237 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 818,
            "unit": "ns/op",
            "extra": "136237 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 214,
            "unit": "B/op",
            "extra": "136237 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_50_miss_50/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "136237 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 29659,
            "unit": "ns/op\t     265 B/op\t       7 allocs/op",
            "extra": "3752 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 29659,
            "unit": "ns/op",
            "extra": "3752 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 265,
            "unit": "B/op",
            "extra": "3752 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 7,
            "unit": "allocs/op",
            "extra": "3752 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 885.7,
            "unit": "ns/op\t     248 B/op\t       5 allocs/op",
            "extra": "125727 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 885.7,
            "unit": "ns/op",
            "extra": "125727 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 248,
            "unit": "B/op",
            "extra": "125727 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMixedPointPruning/hit_90_miss_10/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 5,
            "unit": "allocs/op",
            "extra": "125727 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 89024,
            "unit": "ns/op\t   30369 B/op\t      30 allocs/op",
            "extra": "1274 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 89024,
            "unit": "ns/op",
            "extra": "1274 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 30369,
            "unit": "B/op",
            "extra": "1274 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 30,
            "unit": "allocs/op",
            "extra": "1274 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 2254,
            "unit": "ns/op\t    1960 B/op\t      28 allocs/op",
            "extra": "49282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 2254,
            "unit": "ns/op",
            "extra": "49282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 1960,
            "unit": "B/op",
            "extra": "49282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/narrow/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 28,
            "unit": "allocs/op",
            "extra": "49282 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 94369,
            "unit": "ns/op\t   33401 B/op\t      73 allocs/op",
            "extra": "1210 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 94369,
            "unit": "ns/op",
            "extra": "1210 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 33401,
            "unit": "B/op",
            "extra": "1210 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 73,
            "unit": "allocs/op",
            "extra": "1210 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 6124,
            "unit": "ns/op\t    5048 B/op\t      71 allocs/op",
            "extra": "19041 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 6124,
            "unit": "ns/op",
            "extra": "19041 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 5048,
            "unit": "B/op",
            "extra": "19041 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/medium/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 71,
            "unit": "allocs/op",
            "extra": "19041 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 123111,
            "unit": "ns/op\t   57144 B/op\t     409 allocs/op",
            "extra": "951 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 123111,
            "unit": "ns/op",
            "extra": "951 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 57144,
            "unit": "B/op",
            "extra": "951 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/linear (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 409,
            "unit": "allocs/op",
            "extra": "951 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm)",
            "value": 37182,
            "unit": "ns/op\t   29240 B/op\t     407 allocs/op",
            "extra": "3228 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - ns/op",
            "value": 37182,
            "unit": "ns/op",
            "extra": "3228 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - B/op",
            "value": 29240,
            "unit": "B/op",
            "extra": "3228 times\n4 procs"
          },
          {
            "name": "BenchmarkLSMMultiLevelIteratorBoundsPruning/wide/range_filter (github.com/feichai0017/NoKV/engine/lsm) - allocs/op",
            "value": 407,
            "unit": "allocs/op",
            "extra": "3228 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 10379,
            "unit": "ns/op\t 789.26 MB/s\t    1795 B/op\t      35 allocs/op",
            "extra": "11100 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 10379,
            "unit": "ns/op",
            "extra": "11100 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 789.26,
            "unit": "MB/s",
            "extra": "11100 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 1795,
            "unit": "B/op",
            "extra": "11100 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogAppendEntries (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 35,
            "unit": "allocs/op",
            "extra": "11100 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog)",
            "value": 175.8,
            "unit": "ns/op\t1455.98 MB/s\t     272 B/op\t       2 allocs/op",
            "extra": "674538 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - ns/op",
            "value": 175.8,
            "unit": "ns/op",
            "extra": "674538 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - MB/s",
            "value": 1455.98,
            "unit": "MB/s",
            "extra": "674538 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - B/op",
            "value": 272,
            "unit": "B/op",
            "extra": "674538 times\n4 procs"
          },
          {
            "name": "BenchmarkVLogReadValue (github.com/feichai0017/NoKV/engine/vlog) - allocs/op",
            "value": 2,
            "unit": "allocs/op",
            "extra": "674538 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal)",
            "value": 562.1,
            "unit": "ns/op\t 455.43 MB/s\t     440 B/op\t       9 allocs/op",
            "extra": "213216 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 562.1,
            "unit": "ns/op",
            "extra": "213216 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - MB/s",
            "value": 455.43,
            "unit": "MB/s",
            "extra": "213216 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 440,
            "unit": "B/op",
            "extra": "213216 times\n4 procs"
          },
          {
            "name": "BenchmarkWALAppend (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 9,
            "unit": "allocs/op",
            "extra": "213216 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal)",
            "value": 2331726,
            "unit": "ns/op\t 7475977 B/op\t   40016 allocs/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - ns/op",
            "value": 2331726,
            "unit": "ns/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - B/op",
            "value": 7475977,
            "unit": "B/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkWALReplay (github.com/feichai0017/NoKV/engine/wal) - allocs/op",
            "value": 40016,
            "unit": "allocs/op",
            "extra": "51 times\n4 procs"
          },
          {
            "name": "BenchmarkTouch (github.com/feichai0017/NoKV/thermos)",
            "value": 23.85,
            "unit": "ns/op",
            "extra": "5024467 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchParallel (github.com/feichai0017/NoKV/thermos)",
            "value": 57.28,
            "unit": "ns/op",
            "extra": "2061546 times\n4 procs"
          },
          {
            "name": "BenchmarkTouchAndClamp (github.com/feichai0017/NoKV/thermos)",
            "value": 20.39,
            "unit": "ns/op",
            "extra": "5876420 times\n4 procs"
          },
          {
            "name": "BenchmarkFrequency (github.com/feichai0017/NoKV/thermos)",
            "value": 16.63,
            "unit": "ns/op",
            "extra": "7246562 times\n4 procs"
          },
          {
            "name": "BenchmarkTopN (github.com/feichai0017/NoKV/thermos)",
            "value": 19607532,
            "unit": "ns/op",
            "extra": "6 times\n4 procs"
          },
          {
            "name": "BenchmarkSlidingWindow (github.com/feichai0017/NoKV/thermos)",
            "value": 77.13,
            "unit": "ns/op",
            "extra": "1576416 times\n4 procs"
          },
          {
            "name": "BenchmarkDecay (github.com/feichai0017/NoKV/thermos)",
            "value": 57352,
            "unit": "ns/op",
            "extra": "1905 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 58.72,
            "unit": "ns/op",
            "extra": "2048044 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 156.1,
            "unit": "ns/op",
            "extra": "949804 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 168.6,
            "unit": "ns/op",
            "extra": "830368 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 182.2,
            "unit": "ns/op",
            "extra": "755599 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 39.66,
            "unit": "ns/op",
            "extra": "3179610 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 115.8,
            "unit": "ns/op",
            "extra": "1000000 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 127,
            "unit": "ns/op",
            "extra": "982969 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueConsumerSessionPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 142.2,
            "unit": "ns/op",
            "extra": "804100 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 71.4,
            "unit": "ns/op",
            "extra": "1755622 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 164.6,
            "unit": "ns/op",
            "extra": "813712 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 168.4,
            "unit": "ns/op",
            "extra": "752497 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePushOnlyContention/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 191.9,
            "unit": "ns/op",
            "extra": "672536 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueuePopOnlyReady (github.com/feichai0017/NoKV/utils)",
            "value": 26.06,
            "unit": "ns/op",
            "extra": "4596180 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueFullQueueWake (github.com/feichai0017/NoKV/utils)",
            "value": 263.6,
            "unit": "ns/op",
            "extra": "413350 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 4579,
            "unit": "ns/op",
            "extra": "25874 times\n4 procs"
          },
          {
            "name": "BenchmarkMPSCQueueCloseDrain/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 7866,
            "unit": "ns/op",
            "extra": "15051 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=1 (github.com/feichai0017/NoKV/utils)",
            "value": 53.5,
            "unit": "ns/op",
            "extra": "1943838 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=4 (github.com/feichai0017/NoKV/utils)",
            "value": 75.59,
            "unit": "ns/op",
            "extra": "1574065 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=8 (github.com/feichai0017/NoKV/utils)",
            "value": 75.06,
            "unit": "ns/op",
            "extra": "1604710 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPushPop/producers=16 (github.com/feichai0017/NoKV/utils)",
            "value": 74.58,
            "unit": "ns/op",
            "extra": "1627225 times\n4 procs"
          },
          {
            "name": "BenchmarkRingPopBurst (github.com/feichai0017/NoKV/utils)",
            "value": 9.952,
            "unit": "ns/op",
            "extra": "12039648 times\n4 procs"
          }
        ]
      }
    ]
  }
}