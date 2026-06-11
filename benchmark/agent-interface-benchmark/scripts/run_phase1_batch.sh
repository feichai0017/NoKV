#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_phase1_batch.sh [options]

Options:
  --data-root PATH              Benchmark data root.
  --output-jsonl PATH           Telemetry JSONL path.
  --arm ARM                     Run one arm. May be repeated.
  --task-id TASK_ID             Run one task. Omit to run all fixed Phase 1 tasks.
  --base-profile PATH           Runtime profile YAML. Defaults to benchmark/agent-interface-benchmark/base_profile.yaml.
  --api-surface SURFACE         openai_agents_responses_schema_once or openai_chat_completions.
  --repeat N, --repeats N       Repeat count per selected task. Defaults to base profile.
  --model MODEL                 OpenAI model. Defaults to YANEX_BENCH_MODEL, OPENAI_MODEL, or gpt-5.5.
  --max-completion-tokens N     Max completion tokens. Defaults to base profile.
  --max-turns N                 Max model/tool turns per run.
  --max-tool-calls N            Max tool calls per run.
  --no-live-log                 Disable live JSONL event logging.
  --help                        Show this help.
EOF
}

option_value() {
  local option="$1"
  local raw="${2:-}"
  if [ -z "${raw}" ]; then
    echo "${option} expects a non-empty value" >&2
    exit 2
  fi
  printf '%s' "${raw}"
}

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
data_root="${YANEX_BENCH_DATA_ROOT:-${repo_root}/benchmark/data/yanex-demo}"
run_timestamp="${YANEX_BENCH_TS:-$(date +%Y%m%d-%H%M%S)}"
if [[ "${run_timestamp}" =~ ^([0-9]{8}) ]]; then
  run_date="${BASH_REMATCH[1]}"
else
  run_date="$(date +%Y%m%d)"
fi
output_jsonl="${YANEX_BENCH_OUTPUT_JSONL:-}"
model="${YANEX_BENCH_MODEL:-${OPENAI_MODEL:-gpt-5.5}}"
repeats=""
max_completion_tokens=""
base_profile=""
api_surface=""
task_id=""
max_turns=""
max_tool_calls=""
live_log="${YANEX_BENCH_LIVE_LOG:-1}"
arms=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --data-root)
      data_root="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --output-jsonl)
      output_jsonl="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --arm)
      arms+=("$(option_value "$1" "${2:-}")")
      shift 2
      ;;
    --task-id)
      task_id="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --base-profile)
      base_profile="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --api-surface)
      api_surface="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --repeat|--repeats)
      repeats="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --model)
      model="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --max-completion-tokens)
      max_completion_tokens="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --max-turns)
      max_turns="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --max-tool-calls)
      max_tool_calls="$(option_value "$1" "${2:-}")"
      shift 2
      ;;
    --no-live-log)
      live_log=0
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "${data_root}" ]; then
  echo "--data-root resolved to an empty path" >&2
  exit 2
fi

if [ -z "${output_jsonl}" ]; then
  output_jsonl="${data_root}/results/${run_date}/phase1-${run_timestamp}.jsonl"
fi

if [ -n "${repeats}" ] && { ! [[ "${repeats}" =~ ^[0-9]+$ ]] || [ "${repeats}" -lt 1 ]; }; then
  echo "--repeats expects a positive integer, got ${repeats}" >&2
  exit 2
fi

active_base_profile="${base_profile:-${repo_root}/benchmark/agent-interface-benchmark/base_profile.yaml}"
if [ ! -f "${active_base_profile}" ]; then
  echo "base profile not found: ${active_base_profile}" >&2
  exit 2
fi

if [ -z "${model}" ]; then
  echo "model is required: pass --model MODEL, export YANEX_BENCH_MODEL=MODEL, or export OPENAI_MODEL=MODEL" >&2
  exit 2
fi

if [ -z "${OPENAI_API_KEY-}" ]; then
  echo "OPENAI_API_KEY is required" >&2
  exit 2
fi

cargo_bin="${CARGO:-cargo}"
if ! command -v "${cargo_bin}" >/dev/null 2>&1; then
  if [ -x "${HOME}/.cargo/bin/cargo" ]; then
    cargo_bin="${HOME}/.cargo/bin/cargo"
  else
    echo "cargo is required but was not found in PATH" >&2
    echo "Try: source \"${HOME}/.cargo/env\"" >&2
    exit 127
  fi
fi

default_python_bin="python3"
local_python_bin="${repo_root}/benchmark/agent-interface-benchmark/.venv/bin/python"
if [ -n "${PYTHON:-}" ]; then
  python_bin="${PYTHON}"
elif [ -x "${local_python_bin}" ]; then
  python_bin="${local_python_bin}"
else
  python_bin="${default_python_bin}"
fi

profile_api_surface() {
  local profile_path="$1"
  awk -F: '
    /^[[:space:]]*api_surface[[:space:]]*:/ {
      value=$2
      sub(/^[[:space:]]*/, "", value)
      sub(/[[:space:]]*$/, "", value)
      print value
      exit
    }
  ' "${profile_path}"
}

require_agents_runner_dependencies() {
  if [ "${active_api_surface}" != "openai_agents_responses_schema_once" ]; then
    return 0
  fi
  if ! command -v "${python_bin}" >/dev/null 2>&1; then
    echo "python is required for ${active_api_surface}: ${python_bin} was not found" >&2
    exit 127
  fi
  if ! "${python_bin}" - <<'PY'; then
import importlib.metadata
import re
import sys

requirement = "openai-agents>=0.7.0,<0.8.0"
try:
    version = importlib.metadata.version("openai-agents")
except importlib.metadata.PackageNotFoundError:
    print(
        f"{requirement} is required for openai_agents_responses_schema_once",
        file=sys.stderr,
    )
    sys.exit(1)
match = re.match(r"^(\d+)\.(\d+)\.(\d+)", version)
supported = False
if match is not None:
    parsed = tuple(int(part) for part in match.groups())
    supported = (0, 7, 0) <= parsed < (0, 8, 0)
if not supported:
    print(
        f"{requirement} is required for openai_agents_responses_schema_once; "
        f"installed openai-agents=={version}",
        file=sys.stderr,
    )
    sys.exit(1)
PY
    echo "Install with:" >&2
    echo "  ${python_bin} -m pip install -r ${repo_root}/benchmark/agent-interface-benchmark/agents_runner/requirements.txt" >&2
    exit 2
  fi
}

active_api_surface="${api_surface:-$(profile_api_surface "${active_base_profile}")}"
if [ -z "${active_api_surface}" ]; then
  echo "api_surface is required: pass --api-surface or set api_surface in ${active_base_profile}" >&2
  exit 2
fi
require_agents_runner_dependencies

start_live_monitor() {
  if [ "${live_log}" = "0" ]; then
    return 0
  fi
  if ! command -v "${python_bin}" >/dev/null 2>&1; then
    echo "live log disabled: ${python_bin} was not found" >&2
    return 0
  fi
  "${python_bin}" -u - "${output_jsonl}" <<'PY' &
import json
import os
import sys
import time

path = sys.argv[1]
position = os.path.getsize(path) if os.path.exists(path) else 0
print("", flush=True)
print("Live Events", flush=True)
print("  watching    " + path, flush=True)

def compact(value, limit=240):
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text

def sum_field(rows, field):
    total = 0
    seen = False
    for row in rows:
        value = row.get(field)
        if isinstance(value, int):
            total += value
            seen = True
    return total if seen else None

def format_count(value):
    return f"{value:,}" if isinstance(value, int) else "n/a"

def format_bool(value):
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "n/a"

def run_outcome(row, metrics):
    if row.get("run_error"):
        return "ERROR"
    correctness = row.get("correctness")
    if correctness is True:
        return "PASS"
    if correctness is False:
        return "FAIL"
    task_success = metrics.get("task_success")
    if task_success is True:
        return "PASS"
    if task_success is False:
        return "FAIL"
    return "DONE"

def metric_count(metrics, row, metric_name, row_name):
    value = metrics.get(metric_name)
    if isinstance(value, int):
        return value
    rows = row.get(row_name)
    if isinstance(rows, list):
        return len(rows)
    return None

while True:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            handle.seek(position)
            while True:
                line = handle.readline()
                if not line:
                    break
                position = handle.tell()
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as err:
                    print(f"[live:warn] invalid jsonl line: {err}", flush=True)
                    continue
                record_type = row.get("record_type")
                if record_type == "tool_call_start":
                    print(
                        "  [tool] "
                        f"{row.get('arm_id')}/{row.get('task_id')} "
                        f"r{row.get('repeat_index')} | "
                        f"{row.get('tool_name')} | "
                        f"call_id={row.get('call_id')} | "
                        f"args={compact(row.get('arguments', {}))}",
                        flush=True,
                    )
                elif record_type == "benchmark_run":
                    api_calls = row.get("api_calls") or []
                    metrics = row.get("derived_metrics") or {}
                    total_tokens = sum_field(api_calls, "total_tokens")
                    prompt_tokens = sum_field(api_calls, "prompt_tokens")
                    completion_tokens = sum_field(api_calls, "completion_tokens")
                    reasoning_tokens = sum_field(api_calls, "reasoning_tokens")
                    cached_prompt_tokens = sum_field(api_calls, "cached_prompt_tokens")
                    tool_calls = metric_count(metrics, row, "tool_call_count", "tool_calls")
                    tool_result_tokens = metrics.get("tool_result_tokens")
                    tool_bytes = metrics.get("tool_bytes_read")
                    wall_time_ms = metrics.get("wall_time_ms")
                    wall_seconds = (
                        f"{wall_time_ms / 1000:.1f}s"
                        if isinstance(wall_time_ms, int)
                        else "n/a"
                    )
                    correctness = row.get("correctness")
                    target = (
                        f"{row.get('arm_id')}/{row.get('task_id')} "
                        f"r{row.get('repeat_index')}"
                    )
                    print(
                        "\n"
                        f"[run] {target}\n"
                        f"  outcome       {run_outcome(row, metrics)}\n"
                        f"  correctness   {format_bool(correctness)}\n"
                        f"  total_tokens  {format_count(total_tokens)}\n"
                        f"  tool_calls    {format_count(tool_calls)}\n"
                        f"  api_calls     {format_count(len(api_calls))}\n"
                        f"  tokens        prompt={format_count(prompt_tokens)} "
                        f"completion={format_count(completion_tokens)} "
                        f"reasoning={format_count(reasoning_tokens)} "
                        f"cached_prompt={format_count(cached_prompt_tokens)}\n"
                        f"  tool_io       result_tokens={format_count(tool_result_tokens)} "
                        f"bytes={format_count(tool_bytes)}\n"
                        f"  wall_time     {wall_seconds}\n"
                        f"  run_error     {row.get('run_error')}",
                        flush=True,
                    )
                    judge = row.get("judge") or {}
                    mismatches = judge.get("mismatches") or []
                    if mismatches:
                        print(f"  judge_mismatch {compact(mismatches)}", flush=True)
                elif record_type:
                    print(f"  [event] {record_type}", flush=True)
    except FileNotFoundError:
        pass
    time.sleep(0.25)
PY
  live_monitor_pid=$!
}

stop_live_monitor() {
  if [ -n "${live_monitor_pid:-}" ]; then
    local grace_seconds="${1:-0.5}"
    if [ "${grace_seconds}" != "0" ]; then
      sleep "${grace_seconds}"
    fi
    kill "${live_monitor_pid}" >/dev/null 2>&1 || true
    wait "${live_monitor_pid}" >/dev/null 2>&1 || true
  fi
}

run_one_batch() {
  local arm_arg=()
  if [ "$#" -eq 1 ]; then
    arm_arg=(--arm "$1")
  fi

  local optional_args=()
  if [ -n "${task_id}" ]; then
    optional_args+=(--task-id "${task_id}")
  fi
  if [ -n "${base_profile}" ]; then
    optional_args+=(--base-profile "${base_profile}")
  fi
  if [ -n "${api_surface}" ]; then
    optional_args+=(--api-surface "${api_surface}")
  fi
  optional_args+=(--model "${model}")
  if [ -n "${repeats}" ]; then
    optional_args+=(--repeats "${repeats}")
  fi
  if [ -n "${max_completion_tokens}" ]; then
    optional_args+=(--max-completion-tokens "${max_completion_tokens}")
  fi
  if [ -n "${max_turns}" ]; then
    optional_args+=(--max-turns "${max_turns}")
  fi
  if [ -n "${max_tool_calls}" ]; then
    optional_args+=(--max-tool-calls "${max_tool_calls}")
  fi

  PYTHON="${python_bin}" "${cargo_bin}" run --manifest-path "${repo_root}/benchmark/agent-interface-benchmark/harness/Cargo.toml" -- run-batch \
    --data-root "${data_root}" \
    ${arm_arg+"${arm_arg[@]}"} \
    ${optional_args+"${optional_args[@]}"} \
    --output-jsonl "${output_jsonl}"
}

cat <<EOF
========================================================================
Phase 1 Benchmark
========================================================================

Paths
  data_root   ${data_root}
  output      ${output_jsonl}
  date_dir    ${run_date}
  timestamp   ${run_timestamp}

Runtime
  profile     ${active_base_profile}
  model       ${model}
  api_surface ${active_api_surface}
  max_tokens  ${max_completion_tokens:-profile}
  live_log    ${live_log}

Selection
  arms        ${arms[*]:-all harness arms}
  task_id     ${task_id:-all fixed Phase 1 tasks}
  repeats     ${repeats:-profile} per selected task

------------------------------------------------------------------------
EOF

start_live_monitor
trap stop_live_monitor EXIT
trap 'stop_live_monitor 0; exit 130' INT
trap 'stop_live_monitor 0; exit 143' TERM

if [ "${#arms[@]}" -eq 0 ]; then
  run_one_batch
else
  for arm in "${arms[@]}"; do
    run_one_batch "${arm}"
  done
fi
