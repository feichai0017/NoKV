# FSMetadata Benchmark Results

This directory stores curated fsmeta benchmark outputs that are intended to be
referenced from documentation and reviews.

Raw local benchmark runs should go under `benchmark/data/fsmeta/results/`; that
directory is ignored by Git.

Current result:

| File | Workload | Notes |
|---|---|---|
| `fsmeta_formal_native_vs_generic_20260425T051640Z.csv` | `checkpoint-storm`, `hotspot-fanin` | Stage 1 native-vs-generic run on Docker Compose |

The interpretation for this result is documented in
`docs/notes/2026-04-25-fsmeta-stage1-benchmark-results.md`.

