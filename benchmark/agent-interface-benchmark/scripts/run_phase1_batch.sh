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
  --repeat N, --repeats N       Repeat count per selected task. This never limits task count.
  --model MODEL                 OpenAI model. Defaults to OPENAI_MODEL or gpt-5.5.
  --max-completion-tokens N     Max completion tokens. Defaults to 4096.
  --max-turns N                 Max model/tool turns per run.
  --max-tool-calls N            Max tool calls per run.
  --help                        Show this help.
EOF
}

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
data_root="${YANEX_BENCH_DATA_ROOT:-${repo_root}/benchmark/data/yanex-demo}"
output_jsonl="${YANEX_BENCH_OUTPUT_JSONL:-${data_root}/results/phase1.jsonl}"
model="${OPENAI_MODEL:-gpt-5.5}"
repeats=10
max_completion_tokens=4096
task_id=""
max_turns=""
max_tool_calls=""
arms=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --data-root)
      data_root="$2"
      shift 2
      ;;
    --output-jsonl)
      output_jsonl="$2"
      shift 2
      ;;
    --arm)
      arms+=("$2")
      shift 2
      ;;
    --task-id)
      task_id="$2"
      shift 2
      ;;
    --repeat|--repeats)
      repeats="$2"
      shift 2
      ;;
    --model)
      model="$2"
      shift 2
      ;;
    --max-completion-tokens)
      max_completion_tokens="$2"
      shift 2
      ;;
    --max-turns)
      max_turns="$2"
      shift 2
      ;;
    --max-tool-calls)
      max_tool_calls="$2"
      shift 2
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

if ! [[ "${repeats}" =~ ^[0-9]+$ ]] || [ "${repeats}" -lt 1 ]; then
  echo "--repeats expects a positive integer, got ${repeats}" >&2
  exit 2
fi

run_one_batch() {
  local arm_arg=()
  if [ "$#" -eq 1 ]; then
    arm_arg=(--arm "$1")
  fi

  local optional_args=()
  if [ -n "${task_id}" ]; then
    optional_args+=(--task-id "${task_id}")
  fi
  if [ -n "${max_turns}" ]; then
    optional_args+=(--max-turns "${max_turns}")
  fi
  if [ -n "${max_tool_calls}" ]; then
    optional_args+=(--max-tool-calls "${max_tool_calls}")
  fi

  cargo run --manifest-path "${repo_root}/benchmark/agent-interface-benchmark/harness/Cargo.toml" -- run-batch \
    --data-root "${data_root}" \
    "${arm_arg[@]}" \
    ${optional_args+"${optional_args[@]}"} \
    --model "${model}" \
    --repeats "${repeats}" \
    --max-completion-tokens "${max_completion_tokens}" \
    --output-jsonl "${output_jsonl}"
}

cat <<EOF
Phase 1 benchmark
  data_root:  ${data_root}
  output:     ${output_jsonl}
  model:      ${model}
  repeats:    ${repeats} per selected task
  task_id:    ${task_id:-all fixed Phase 1 tasks}
  arms:       ${arms[*]:-all harness arms}
EOF

if [ "${#arms[@]}" -eq 0 ]; then
  run_one_batch
else
  for arm in "${arms[@]}"; do
    run_one_batch "${arm}"
  done
fi
