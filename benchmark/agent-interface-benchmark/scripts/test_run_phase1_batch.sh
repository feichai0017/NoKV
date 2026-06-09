#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script_under_test="${script_dir}/run_phase1_batch.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

repo_root="${tmp_dir}/repo"
mkdir -p "${repo_root}/benchmark/agent-interface-benchmark/.venv/bin"
mkdir -p "${repo_root}/benchmark/agent-interface-benchmark/harness"
mkdir -p "${repo_root}/data"

cat >"${repo_root}/benchmark/agent-interface-benchmark/base_profile.yaml" <<'YAML'
api_surface: openai_agents_responses_schema_once
run_policy:
  repeats_per_arm_task: 1
YAML

fake_python="${repo_root}/benchmark/agent-interface-benchmark/.venv/bin/python"
cat >"${fake_python}" <<'SH'
#!/usr/bin/env bash
if [ "${1:-}" = "-u" ]; then
  exit 0
fi
while IFS= read -r _line; do
  :
done
exit 0
SH
chmod +x "${fake_python}"

cargo_env_file="${tmp_dir}/cargo-python-env"
fake_cargo="${tmp_dir}/cargo"
cat >"${fake_cargo}" <<SH
#!/usr/bin/env bash
printf '%s' "\${PYTHON-}" >"${cargo_env_file}"
exit 0
SH
chmod +x "${fake_cargo}"

(
  cd "${repo_root}"
  env -u PYTHON \
    OPENAI_API_KEY=unit-test-key \
    CARGO="${fake_cargo}" \
    "${script_under_test}" \
      --data-root "${repo_root}/data" \
      --arm nokv_native_v1 \
      --task-id status_counts \
      --repeats 1 \
      --model unit-model \
      --no-live-log
)

actual_python="$(cat "${cargo_env_file}")"
if [ "${actual_python}" != "${fake_python}" ]; then
  echo "expected wrapper to pass local venv python to cargo" >&2
  echo "expected: ${fake_python}" >&2
  echo "actual: ${actual_python:-<unset>}" >&2
  exit 1
fi
