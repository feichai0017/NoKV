# Yanex x NoKV Agent Interface Benchmark

日期：2026-06-04

This directory defines the fixed prompt assets for a demo-oriented benchmark over a Yanex experiment corpus. The benchmark follows the demo proposal direction: compare agent-facing interfaces over the same experiment state, not storage engines in isolation.

## Current Goal

We want to measure how different interface shapes affect an external agent's task cost and evidence quality over the same Yanex tracking corpus.

The core question is not "which backend is faster." The core question is:

- Same model, same tasks, same corpus snapshot: how do different tool surfaces change token cost, reasoning tokens, tool calls, bytes read, latency, and evidence precision?
- Does NoKV's namespace-native metadata surface help evidence-oriented exploration compared with raw SQLite, SQLite-backed filesystem projection, and the NoKV FUSE/POSIX view?

This keeps the demo doc's direction intact. The profile just maps that direction onto the four concrete arms for this branch:

- Raw SQLite: structured relational interface.
- SQLite AgentFS: SQLite-backed filesystem-shaped projection.
- NoKV POSIX FUSE: this branch's FUSE frontend over the NoKV corpus.
- NoKV native namespace: NoKV SDK/service namespace tools with metadata and generations.

We are not adding a separate semantic HandleFS arm in this profile. Stable semantic handles can be developed later on top of the native namespace arm.

## Corpus

Yanex is a lightweight experiment tracking system. The relevant corpus fields are experiment metadata, params, metrics, dependencies, artifacts, stdout/stderr, and git state.

Corpus archive:

`/Users/wangchanghao/Downloads/yanex-experiment-metadata-origami-data-gen-project.tar.gz`

Observed corpus facts:

- 875 experiment directories.
- 875 `metadata.json` files.
- 872 `metrics.json` files.
- 869 `params.yaml` files.
- 842 `dependencies.json` files.
- Mostly `completed`, with a small number of `cancelled` runs.
- Common scripts include `eval.py`, `sample.py`, `train.py`, `sample_tabdiff.py`, and `split.py`.
- Common tags include `sweep`, `origami`, `final`, and `tabdiff`.
- Some metadata references missing git diff artifacts. This is useful for testing whether agents report missing evidence instead of inventing it.

The corpus is a fixed historical snapshot. The MVP can ETL it into SQLite and NoKV. Yanex runtime integration is only needed for later live tracking demos.

## Benchmark Mode

The benchmark uses Warm Interface Mode.

Each run receives:

1. `base_profile.yaml`
2. One arm card from `arms/`
3. One benchmark task prompt
4. The matching arm-specific tool definitions registered by the harness

Rules:

- Clear all model message history after each run.
- Do not carry state from another task, arm, replicate, or previous attempt.
- Keep the model, temperature, max tokens, tool-call budget, and retry policy fixed across arms.
- Keep every arm backed by the same Yanex corpus snapshot.
- Do not include benchmark answers, task-specific hints, or known solving run IDs in arm cards.

Cold Interface Mode is not the MVP target. Cold mode measures interface discoverability; this benchmark is trying to measure execution cost once the agent understands the available interface.

## OpenAI SDK/API Fit

The ordinary OpenAI Chat Completions API is sufficient for the first harness.

- Explicit `messages` arrays support stateless runs.
- Tool/function calling supports arm-specific tool surfaces.
- Non-streaming responses expose usage fields for token accounting.
- Structured JSON output can be enforced with `response_format` on supported models.
- The harness should record request IDs, model names, timestamps, usage, tool calls, and tool results.

OpenAI does not return dollar cost as a canonical per-run field. The harness should compute cost from recorded usage and a pinned pricing table.

Initial runs should be non-streaming so final usage telemetry is complete. Responses API can remain a later backend option. OpenAI Evals and the Agents SDK are not required for this MVP.

## Files

- `base_profile.yaml`: shared agent behavior, output schema, and telemetry contract.
- `arms/sqlite_raw.yaml`: raw SQLite arm.
- `arms/sqlite_agentfs.yaml`: SQLite AgentFS arm.
- `arms/nokv_posix.yaml`: NoKV POSIX FUSE arm.
- `arms/nokv_native.yaml`: NoKV native namespace arm.

## Arms

### Raw SQLite

Tools:

- `show_schema`
- `query_sql`
- `read_blob`

Use this arm for structured filtering, aggregation, joins, ranking, and top-k analysis.

Evidence examples:

- `sqlite://experiments/{experiment_id}`
- `sqlite://metrics/{experiment_id}/{metric_name}/{step}`
- `sqlite://artifacts/{experiment_id}/{artifact_path}`

### SQLite AgentFS

Tools:

- `ls`
- `stat`
- `read`
- `grep`
- `find`

Layout:

- `/runs/{experiment_id}/metadata.json`
- `/runs/{experiment_id}/params.yaml`
- `/runs/{experiment_id}/metrics.json`
- `/runs/{experiment_id}/dependencies.json`
- `/runs/{experiment_id}/artifacts/`
- `/index/...`

This arm tests a filesystem-shaped projection backed by SQLite. It must not call raw SQL tools.

### NoKV POSIX FUSE

Tools:

- `ls`
- `stat`
- `read`
- `grep`
- `find`

Layout:

- `/yanex/runs/{experiment_id}/...`
- `/yanex/index/...`

This arm uses the NoKV FUSE frontend in this branch. It exposes POSIX-visible metadata such as type, size, mode, mtime, and inode. It does not expose NoKV body descriptors, metadata generations, snapshots, watches, or semantic handles.

### NoKV Native Namespace

Tools:

- `list`
- `stat`
- `read`

This arm uses NoKV's native namespace SDK/service surface. It can expose ReadDirPlus-style entries, inode metadata, body descriptors, digest URI, manifest ID, and generation.

Evidence examples:

- `nokv-native:///yanex/runs/{experiment_id}/metadata.json@generation:{generation}`
- `nokv-native:///yanex/runs/{experiment_id}/metrics.json@generation:{generation}#{metric_name}`

Important constraint: this MVP is read-only. Native snapshot creation and watch/replay are intentionally out of scope until a task group explicitly requires them.

## Metrics

Record both interface-normalized and all-in metrics.

- `interface_card_tokens`
- `task_prompt_tokens`
- `reasoning_tokens`
- `answer_tokens`
- `tool_call_count`
- `tool_result_tokens`
- `tool_bytes_read`
- `wall_time_ms`
- `execution_cost_usd`
- `all_in_cost_usd`
- `evidence_precision`
- `task_success`

Primary token-efficiency claims should use `execution_cost_usd`, `reasoning_tokens`, `tool_result_tokens`, and `tool_call_count`. Keep `all_in_cost_usd` as a sensitivity check because arm cards have different lengths.

## Implementation Plan

The first PR should be a harness skeleton only.

1. Read `base_profile.yaml` and one arm card.
2. Load a fixed task JSON/YAML.
3. Build a stateless Chat Completions request.
4. Support mock or local tool execution.
5. Write per-run telemetry JSONL.
6. Add a unit test that needs no OpenAI key and verifies prompt assembly, stateless message construction, and telemetry schema.

Next increments:

1. ETL Yanex corpus into SQLite and implement raw SQLite tools.
2. Implement SQLite AgentFS projection tools and compare with raw SQLite on the same tasks.
3. Ingest the corpus into NoKV and implement the NoKV POSIX FUSE arm.
4. Implement NoKV native namespace tools: `list/stat/read`.
5. Add live-tracking tools only when a task group explicitly requires live state changes.

## Fairness And Risks

- Do not add task-specific helper tools such as `find_best_run`.
- Do not let NoKV native expose more business semantics than the other arms.
- Keep `/index/...` files equivalent between SQLite AgentFS and NoKV POSIX if they are generated.
- Do not silently fill missing artifacts or missing git diffs.
- Keep all arms on the same corpus snapshot.
- Pin the model snapshot and pricing table for reproducibility.
- Allow `reasoning_tokens` to be absent because model/API support varies.
- Design task groups so raw SQLite can win structured analytics tasks; NoKV native should be tested where namespace metadata, body descriptors, and generation stability matter.
