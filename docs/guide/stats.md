<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Stats and Observability

Every runtime component should expose stats at its owner boundary, then convert
to generic maps only at diagnostics edges.

## Active Domains

| Domain | Owner |
|---|---|
| fsmeta executor | `fsmeta/exec` |
| local runtime | `fsmeta/runtime/local` |
| fsmeta watches/snapshots | `fsmeta/observe` and runtime adapters |
| root truth | `meta/root` |
| coordinator | `coordinator` |
| Rust data plane | `raftstore` |

The local server exposes expvar and pprof through `--metrics-addr`.

```bash
curl http://127.0.0.1:9400/debug/vars
```

Do not put Badger or Holt-specific internals into generic fsmeta stats unless
the field has the same meaning across backends.
