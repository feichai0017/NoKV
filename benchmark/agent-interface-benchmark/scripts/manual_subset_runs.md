# Phase 1 Benchmark CLI Control

This is the central command-line reference for controlling Phase 1 benchmark
runs. Keep it focused on reusable knobs and composable command shapes, not on
one-off experiment batches.

All commands assume the repository root is the current directory.

## Environment

```bash
export OPENAI_API_KEY=...
export OPENAI_MODEL=gpt-5.5

export DATA_ROOT=/Users/wangchanghao/NoKV/benchmark/data/yanex-demo
```

The wrapper script owns model selection. It uses `--model MODEL` when provided,
then `YANEX_BENCH_MODEL`, then `OPENAI_MODEL`, and finally `gpt-5.5`. The base
profile does not configure the model name. The script checks the model and API
key before invoking Cargo.

The default API surface uses the OpenAI Agents SDK runner. Install its Python
dependency in the local benchmark virtual environment. The wrapper
automatically uses this environment when `PYTHON` is not set:

```bash
python3.12 -m venv benchmark/agent-interface-benchmark/.venv
benchmark/agent-interface-benchmark/.venv/bin/python -m pip install -r benchmark/agent-interface-benchmark/agents_runner/requirements.txt
```

If `cargo` is not on `PATH`, load the Rust environment first:

```bash
source "${HOME}/.cargo/env"
```

Verify the prepared data root before spending model calls:

```bash
cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- verify \
  --data-root "${DATA_ROOT}"
```

Inspect available tasks and arm tool surfaces:

```bash
cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- list-tasks

cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- tools \
  --arm sqlite_raw_v1

cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- tools \
  --arm nokv_native_v1
```

## Control Model

Use `benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh` as the
normal entry point. It forwards to the harness `run-batch` command.

The benchmark is controlled by these axes:

- `--arm ARM`: select one arm. Repeat the flag for multiple arms. Omit it to
  run all harness arms.
- `--task-id TASK_ID`: select one task. Omit it to run all fixed Phase 1 tasks.
- `--repeats N`: repeat count per selected arm/task pair.
- `--output-jsonl PATH`: append telemetry records to this JSONL. Omit it to
  use `${DATA_ROOT}/results/YYYYMMDD/phase1-${timestamp}.jsonl`.
- `--base-profile PATH`: use a non-default runtime profile.
- `--api-surface SURFACE`: override the profile API surface.
- `--model MODEL`: override the script-level model default.
- `--max-completion-tokens N`: override profile completion budget.
- `--max-turns N`: override profile model/tool turn budget.
- `--max-tool-calls N`: override profile tool-call budget.
- `--no-live-log`: disable live JSONL event logging.

To run any custom subset, choose the arm set, choose either all tasks or one
task, choose repeats, and choose an output file. If a multi-task custom subset
is needed, run the same command once per task with the same output JSONL.

## Runtime Profile

`benchmark/agent-interface-benchmark/base_profile.yaml` is the default runtime
profile. The harness reads this file at runtime. Pass `--base-profile PATH` to
use a different profile.

The harness parses these runtime fields:

- `model.temperature`
- `model.max_completion_tokens`
- `model.stream`
- `model.structured_output`
- `run_policy.repeats_per_arm_task`
- `run_policy.max_turns`
- `run_policy.max_tool_calls`
- `run_policy.tool_call_timeout_ms`
- `base_system_message`
- `base_developer_message`

Command-line values override the profile for repeats, max turns, max tool
calls, and max completion tokens. The wrapper script always passes a model to
the harness. If those runtime flags are omitted, the profile defaults are used.
Each run injects only the parsed base system message, parsed
base developer message, current arm card, and current task prompt into the
request context. Full profile YAML, rubric YAML, and gold SQL are judge or
harness-side data and are not exposed to the agent.

## Live Logging

The wrapper tails the active JSONL by default while the benchmark is running.
It prints:

- `tool_call_start` as indented `[tool]` lines, including arm, task, repeat,
  tool name, call id, and arguments.
- completed `benchmark_run` rows as `[run]` blocks. The first lines show
  outcome, correctness, total tokens, and tool calls. The later lines show API
  call count, prompt tokens, completion tokens, reasoning tokens, cached prompt
  tokens, tool result tokens, tool bytes, wall time, and run error.
- judge mismatches as indented `judge_mismatch` lines under the run block.

OpenAI retry diagnostics emitted by the harness still appear on stderr. Hidden
reasoning text is not exposed by the Responses API; the live log reports only
the returned reasoning token counts.

Disable live logs when needed:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --arm nokv_native_v1 \
  --task-id train_top_configs_report \
  --repeats 1 \
  --no-live-log
```

## Command Shapes

Run one task on one arm:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --arm nokv_native_v1 \
  --task-id train_top_configs_report \
  --repeats 1
```

Run one task on two arms:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --arm sqlite_raw_v1 \
  --arm nokv_native_v1 \
  --task-id train_top_configs_report \
  --repeats 1
```

Run all fixed Phase 1 tasks on one arm:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --arm nokv_native_v1 \
  --repeats 1
```

Run all fixed Phase 1 tasks on two arms:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --arm sqlite_raw_v1 \
  --arm nokv_native_v1 \
  --repeats 1
```

Run with explicit runtime overrides:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --arm nokv_native_v1 \
  --task-id train_top_configs_report \
  --repeats 1 \
  --model "${OPENAI_MODEL}" \
  --max-completion-tokens 4096 \
  --max-turns 20 \
  --max-tool-calls 80
```

Run with an alternate profile:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --base-profile /path/to/base_profile.yaml \
  --arm nokv_native_v1 \
  --task-id train_top_configs_report
```

## Sensitivity Runs

Keep runtime-sensitivity outputs separate from primary comparison outputs. For
example, a completion-budget sensitivity run should use its own JSONL:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --data-root "${DATA_ROOT}" \
  --arm nokv_native_v1 \
  --task-id train_top_configs_report \
  --repeats 1 \
  --max-completion-tokens 2048 \
  --output-jsonl "${DATA_ROOT}/results/phase1-max-tokens-2048-$(date +%Y%m%d-%H%M%S).jsonl"
```

## JSONL Inspection

This prints completed `benchmark_run` rows. Partial `tool_call_start` records
mean a run was interrupted or is still in progress.

```bash
python3 - <<'PY' /path/to/output.jsonl
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(f"missing JSONL: {path}")

for line in path.read_text().splitlines():
    row = json.loads(line)
    if row.get("record_type") != "benchmark_run":
        continue
    metrics = row.get("derived_metrics", {})
    print(
        row.get("arm_id"),
        row.get("task_id"),
        "repeat=" + str(row.get("repeat_index")),
        "success=" + str(metrics.get("task_success")),
        "api_calls=" + str(len(row.get("api_calls", []))),
        "tool_calls=" + str(metrics.get("tool_call_count")),
        "tool_result_tokens=" + str(metrics.get("tool_result_tokens")),
        "bytes=" + str(metrics.get("tool_bytes_read")),
        "run_error=" + str(row.get("run_error")),
    )
PY
```
