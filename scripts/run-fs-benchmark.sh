#!/usr/bin/env bash
#
# NoKV-FS benchmark framework — the single entry point.
#
# Runs the L2 mount comparison as a labeled matrix: NoKV local metadata tier ×
# concurrency sweep × {native juicefs-bench-shaped driver, real fio/mdtest},
# plus JuiceFS over the same RustFS endpoint. Every row is boundary/tier/cache
# labeled (canonical schema, see bench/drivers/schema.py), so the headline is
# always L2-vs-L2 under a declared tier — never NoKV's faster service path
# against JuiceFS's mount.
#
# This is a local engineering baseline, not an official MLPerf result.
#
# Modes:
#   (default)   quick   one NoKV tier (direct/WAL) + JuiceFS, concurrency 1
#   --matrix            NoKV local tier + JuiceFS, full sweep
#
# Tools are gated: absent fio/mdtest/mpirun surface as explicit tool-missing
# rows. Required infra tools (rustfs/redis/juicefs/aws/python) must be present.

set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MODE="quick"
RESULT_CSV=""
AGGREGATE_CSV=""
ENV_JSON=""
DECOMPOSE_CSV=""
BASELINE=""
PRODUCT_WORKLOADS_OVERRIDE=""
PRIMITIVE_WORKLOADS_OVERRIDE=""
CONCURRENCY_OVERRIDE=""
TIERS_OVERRIDE=""
CACHE_STATES_OVERRIDE=""
REPEATS="${NOKV_BENCH_REPEATS:-1}"
DECOMPOSE=0
SKIP_REAL_TOOLS=0

usage() {
    cat <<EOF
Usage: scripts/run-fs-benchmark.sh [--matrix|--quick] [options]

  --matrix                 full grid (NoKV local tier, concurrency sweep)
  --quick                  one tier (direct/WAL), concurrency 1 (default)
  --result-csv PATH        also write the raw labeled CSV here
  --aggregate-csv PATH     also write median/p95 aggregate CSV here
  --env-json PATH          also write environment/version metadata JSON here
  --decompose              capture NoKV /stats before/after each measured phase
  --decompose-csv PATH     also write decompose sidecar CSV here
  --repeats N              repeat the selected grid N times (default: $REPEATS)
  --baseline PATH          compare against a baseline CSV and fail on regression
  --product-workloads LIST product workloads
                           metadata/checkpoint/training_read run once at p=1;
                           ai_shard_range_read is swept across concurrency
                           (default: metadata_create_list,checkpoint,training_read; use none to skip)
  --primitive-workloads LIST
                           primitive workloads swept across concurrency
                           (default: bigfile,smallfile,metadata; use none to skip)
  --skip-real-tools        skip fio/mdtest/juicefs-bench rows
  --concurrency "1 4 16"   override the concurrency sweep
  --tiers "local"          override NoKV tiers (current CLI supports local)
  --cache-states LIST      read phases to emit: cold,warm,hot (default: cold,warm)

Configuration is via NOKV_BENCH_* env vars; see scripts/lib/fs-bench-common.sh.
Profile: NOKV_BENCH_PROFILE=smoke|standard|long (default smoke).
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
    --matrix) MODE="matrix" ;;
    --quick) MODE="quick" ;;
    --result-csv) RESULT_CSV="$2"; shift ;;
    --aggregate-csv) AGGREGATE_CSV="$2"; shift ;;
    --env-json) ENV_JSON="$2"; shift ;;
    --decompose) DECOMPOSE=1 ;;
    --decompose-csv) DECOMPOSE_CSV="$2"; shift ;;
    --repeats) REPEATS="$2"; shift ;;
    --baseline) BASELINE="$2"; shift ;;
    --product-workloads) PRODUCT_WORKLOADS_OVERRIDE="$2"; shift ;;
    --primitive-workloads) PRIMITIVE_WORKLOADS_OVERRIDE="$2"; shift ;;
    --skip-real-tools) SKIP_REAL_TOOLS=1 ;;
    --concurrency) CONCURRENCY_OVERRIDE="$2"; shift ;;
    --tiers) TIERS_OVERRIDE="$2"; shift ;;
    --cache-states) CACHE_STATES_OVERRIDE="$2"; shift ;;
    -h | --help) usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage; exit 2 ;;
    esac
    shift
done
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || { echo "error: --repeats must be a positive integer" >&2; exit 2; }
[[ -n "$DECOMPOSE_CSV" ]] && DECOMPOSE=1

# shellcheck source=scripts/lib/fs-bench-common.sh
source "$ROOT_DIR/scripts/lib/fs-bench-common.sh"
[[ -n "$CACHE_STATES_OVERRIDE" ]] && CACHE_STATES="$CACHE_STATES_OVERRIDE"
export NOKV_BENCH_CACHE_STATES="$CACHE_STATES"

# Profile-derived workload shape (drives the native driver's product-thesis set).
case "$PROFILE" in
smoke) DATASET_DIRS=8 FILES_PER_DIR=64 SAMPLE_BYTES=512 CHECKPOINT_BYTES=4096 CHECKPOINT_STEPS=32 ;;
standard) DATASET_DIRS=32 FILES_PER_DIR=256 SAMPLE_BYTES=$((16 * 1024)) CHECKPOINT_BYTES=$((1024 * 1024)) CHECKPOINT_STEPS=256 ;;
long) DATASET_DIRS=64 FILES_PER_DIR=1024 SAMPLE_BYTES=$((256 * 1024)) CHECKPOINT_BYTES=$((8 * 1024 * 1024)) CHECKPOINT_STEPS=1024 ;;
*) echo "error: NOKV_BENCH_PROFILE must be smoke|standard|long" >&2; exit 2 ;;
esac
export DATASET_DIRS FILES_PER_DIR SAMPLE_BYTES CHECKPOINT_BYTES CHECKPOINT_STEPS

# Matrix dimensions. Most product-thesis workloads are latency rows and run once
# at p=1. `ai_shard_range_read` is a training data-plane throughput row, so it
# follows the same concurrency sweep as FS primitives and real tools.
PRODUCT_WORKLOADS="${PRODUCT_WORKLOADS_OVERRIDE:-metadata_create_list,checkpoint,training_read}"
PRIMITIVE_WORKLOADS="${PRIMITIVE_WORKLOADS_OVERRIDE:-bigfile,smallfile,metadata}"
[[ "$PRODUCT_WORKLOADS" == "none" ]] && PRODUCT_WORKLOADS=""
[[ "$PRIMITIVE_WORKLOADS" == "none" ]] && PRIMITIVE_WORKLOADS=""
bench_without_workloads() {
    local input="$1" excluded="$2" result="" raw name
    IFS=',' read -ra names <<<"$input"
    for raw in "${names[@]}"; do
        name="${raw//[[:space:]]/}"
        [[ -z "$name" ]] && continue
        case ",$excluded," in
        *,"$name",*) ;;
        *) [[ -z "$result" ]] && result="$name" || result="${result},${name}" ;;
        esac
    done
    echo "$result"
}

bench_only_workloads() {
    local input="$1" included="$2" result="" raw name
    IFS=',' read -ra names <<<"$input"
    for raw in "${names[@]}"; do
        name="${raw//[[:space:]]/}"
        [[ -z "$name" ]] && continue
        case ",$included," in
        *,"$name",*) [[ -z "$result" ]] && result="$name" || result="${result},${name}" ;;
        *) ;;
        esac
    done
    echo "$result"
}

PRODUCT_THROUGHPUT_WORKLOADS="$(bench_only_workloads "$PRODUCT_WORKLOADS" "ai_shard_range_read")"
PRODUCT_LATENCY_WORKLOADS="$(bench_without_workloads "$PRODUCT_WORKLOADS" "ai_shard_range_read")"
NOKV_TOOLS="fio,mdtest"
JUICEFS_TOOLS="fio,mdtest,juicefs-bench"
if [[ "$MODE" == "matrix" ]]; then
    CONCURRENCY_SWEEP="${CONCURRENCY_OVERRIDE:-1 4 16}"
    NOKV_TIERS="${TIERS_OVERRIDE:-local}"
else
    CONCURRENCY_SWEEP="${CONCURRENCY_OVERRIDE:-1}"
    NOKV_TIERS="${TIERS_OVERRIDE:-local}"
fi
[[ "$PROFILE" == "smoke" && -z "$CONCURRENCY_OVERRIDE" && "$MODE" == "matrix" ]] && CONCURRENCY_SWEEP="1 4"
REAL_TOOLS_MODE="default"
[[ "$SKIP_REAL_TOOLS" == "1" ]] && REAL_TOOLS_MODE="skip"

HOST_LABEL="$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo host)"
ENV_ID="${NOKV_BENCH_ENV_ID:-$(date -u +%Y%m%dT%H%M%SZ)-${HOST_LABEL}}"
export NOKV_BENCH_ENV_ID="$ENV_ID"

bench_require_tools
bench_build_nokv
bench_make_workdir
trap bench_cleanup EXIT INT TERM
bench_start_backends

RUN_CSV="$WORKDIR/run.csv"
"$PYTHON_BIN" -c "import sys; sys.path.insert(0, '$ROOT_DIR/bench/drivers'); import schema; print(schema.header())" >"$RUN_CSV"
ENV_JSON_RUN="$WORKDIR/env.json"
"$PYTHON_BIN" "$ROOT_DIR/scripts/fs-bench-env.py" \
    --out "$ENV_JSON_RUN" --env-id "$ENV_ID" --mode "$MODE" --profile "$PROFILE" \
    --tiers "$NOKV_TIERS" --concurrency "$CONCURRENCY_SWEEP" \
    --product-workloads "$PRODUCT_WORKLOADS" --primitive-workloads "$PRIMITIVE_WORKLOADS" \
    --real-tools "$REAL_TOOLS_MODE" \
    --range-stride "$RANGE_STRIDE" --range-coalesce-gap-bytes "$RANGE_COALESCE_GAP_BYTES" \
    --cache-states "$CACHE_STATES" \
    --repeats "$REPEATS"

DECOMPOSE_RUN_CSV="$WORKDIR/decompose.csv"
if [[ "$DECOMPOSE" == "1" ]]; then
    DECOMPOSE_SNAPSHOT_DIR="$WORKDIR"
    if [[ -n "$DECOMPOSE_CSV" ]]; then
        DECOMPOSE_SNAPSHOT_DIR="${DECOMPOSE_CSV%.csv}.snapshots"
        mkdir -p "$DECOMPOSE_SNAPSHOT_DIR"
    fi
    echo "run_id,env_id,system,metadata_tier,concurrency,tool,workloads,cost_breakdown,before_snapshot,after_snapshot" >"$DECOMPOSE_RUN_CSV"
fi

echo "Running NoKV-FS benchmark mode=$MODE profile=$PROFILE repeats=$REPEATS tiers='$NOKV_TIERS' concurrency='$CONCURRENCY_SWEEP'" >&2
echo "Native workloads product='$PRODUCT_WORKLOADS' product_throughput='$PRODUCT_THROUGHPUT_WORKLOADS' primitive='$PRIMITIVE_WORKLOADS' real_tools=$REAL_TOOLS_MODE" >&2

bench_run_phase() {
    local system="$1" tier="$2" concurrency="$3" tool="$4" workloads="$5"
    shift 5
    if [[ "$DECOMPOSE" != "1" || "$system" != "nokv" ]]; then
        "$@"
        return
    fi

    local label run_label before after breakdown status stats_url
    run_label="${NOKV_BENCH_RUN_ID:-run}"
    label="${run_label}-${system}-${tier}-p${concurrency}-${tool}-${workloads}"
    label="${label//[^A-Za-z0-9_.-]/_}"
    before="$DECOMPOSE_SNAPSHOT_DIR/${label}-before.json"
    after="$DECOMPOSE_SNAPSHOT_DIR/${label}-after.json"
    stats_url="http://${SERVER_ADDRESS}/stats"
    if [[ -n "${NOKV_MOUNT_STATS_ADDRESS:-}" ]]; then
        stats_url="http://${NOKV_MOUNT_STATS_ADDRESS}/stats"
    fi
    "$PYTHON_BIN" "$ROOT_DIR/bench/drivers/decompose.py" \
        --snapshot "$stats_url" --out "$before" >/dev/null 2>&1 || {
        "$@"
        return
    }
    set +e
    "$@"
    status=$?
    set -e
    "$PYTHON_BIN" "$ROOT_DIR/bench/drivers/decompose.py" \
        --snapshot "$stats_url" --out "$after" >/dev/null 2>&1 || true
    breakdown=""
    if [[ -f "$before" && -f "$after" ]]; then
        breakdown="$("$PYTHON_BIN" "$ROOT_DIR/bench/drivers/decompose.py" --before "$before" --after "$after" 2>/dev/null || true)"
    fi
    "$PYTHON_BIN" - "$DECOMPOSE_RUN_CSV" "${NOKV_BENCH_RUN_ID:-}" "$ENV_ID" "$system" "$tier" "$concurrency" "$tool" "$workloads" "$breakdown" "$before" "$after" <<'PY'
import csv
import sys

path, *values = sys.argv[1:]
with open(path, "a", newline="") as handle:
    csv.writer(handle, lineterminator="\n").writerow(values)
PY
    return "$status"
}

run_nokv_tier() {
    local mode="$1" sync="${2:-none}" tier c
    tier="$(bench_tier_label "$mode" "$sync")"
    bench_start_nokv_server "$mode" "$sync"
    bench_mount_nokv warm
    # Latency product-thesis workloads: run once at p=1.
    if [[ -n "$PRODUCT_LATENCY_WORKLOADS" ]]; then
        bench_drop_caches
        bench_run_phase "nokv" "$tier" 1 "native" "$PRODUCT_LATENCY_WORKLOADS" \
            bench_run_native "nokv" "$NOKV_MOUNT" "$tier" 1 0 "$PRODUCT_LATENCY_WORKLOADS" >>"$RUN_CSV"
    fi
    # Throughput product-thesis workloads + FS primitives + real tools:
    # concurrency sweep.
    for c in $CONCURRENCY_SWEEP; do
        if [[ -n "$PRODUCT_THROUGHPUT_WORKLOADS" ]]; then
            bench_drop_caches
            bench_run_phase "nokv" "$tier" "$c" "native" "$PRODUCT_THROUGHPUT_WORKLOADS" \
                bench_run_native "nokv" "$NOKV_MOUNT" "$tier" "$c" 0 "$PRODUCT_THROUGHPUT_WORKLOADS" >>"$RUN_CSV"
        fi
        if [[ -n "$PRIMITIVE_WORKLOADS" ]]; then
            bench_drop_caches
            bench_run_phase "nokv" "$tier" "$c" "native" "$PRIMITIVE_WORKLOADS" \
                bench_run_native "nokv" "$NOKV_MOUNT" "$tier" "$c" 0 "$PRIMITIVE_WORKLOADS" >>"$RUN_CSV"
        fi
        if [[ "$SKIP_REAL_TOOLS" != "1" ]]; then
            bench_run_phase "nokv" "$tier" "$c" "real-tools" "$NOKV_TOOLS" \
                bench_run_real_tools "nokv" "$NOKV_MOUNT" "$tier" "$c" "$NOKV_TOOLS" 0 >>"$RUN_CSV"
        fi
    done
    bench_unmount_nokv
    bench_stop_nokv_server
}

for repeat in $(seq 1 "$REPEATS"); do
    export NOKV_BENCH_RUN_ID="run-${repeat}"
    echo "Benchmark repeat ${repeat}/${REPEATS}" >&2
    for mode in $NOKV_TIERS; do
        run_nokv_tier "$mode" none
    done

    bench_mount_juicefs
    if [[ -n "$PRODUCT_LATENCY_WORKLOADS" ]]; then
        bench_drop_caches
        bench_run_native "juicefs" "$JUICEFS_MOUNT" "juicefs-redis" 1 0 "$PRODUCT_LATENCY_WORKLOADS" >>"$RUN_CSV"
    fi
    for c in $CONCURRENCY_SWEEP; do
        if [[ -n "$PRODUCT_THROUGHPUT_WORKLOADS" ]]; then
            bench_drop_caches
            bench_run_native "juicefs" "$JUICEFS_MOUNT" "juicefs-redis" "$c" 0 "$PRODUCT_THROUGHPUT_WORKLOADS" >>"$RUN_CSV"
        fi
        if [[ -n "$PRIMITIVE_WORKLOADS" ]]; then
            bench_drop_caches
            bench_run_native "juicefs" "$JUICEFS_MOUNT" "juicefs-redis" "$c" 0 "$PRIMITIVE_WORKLOADS" >>"$RUN_CSV"
        fi
        if [[ "$SKIP_REAL_TOOLS" != "1" ]]; then
            bench_run_real_tools "juicefs" "$JUICEFS_MOUNT" "juicefs-redis" "$c" "$JUICEFS_TOOLS" 0 >>"$RUN_CSV"
        fi
    done
    bench_unmount_juicefs
done
unset NOKV_BENCH_RUN_ID

cat "$RUN_CSV"
if [[ -n "$RESULT_CSV" ]]; then
    mkdir -p "$(dirname "$RESULT_CSV")"
    cp "$RUN_CSV" "$RESULT_CSV"
    echo "wrote $RESULT_CSV" >&2
fi

SUMMARY_CSV="$RUN_CSV"
COMPARE_CSV="$RUN_CSV"
AGGREGATE_RUN_CSV=""
if [[ -n "$AGGREGATE_CSV" || "$REPEATS" -gt 1 ]]; then
    AGGREGATE_RUN_CSV="${AGGREGATE_CSV:-$WORKDIR/aggregate.csv}"
    mkdir -p "$(dirname "$AGGREGATE_RUN_CSV")"
    "$PYTHON_BIN" "$ROOT_DIR/scripts/aggregate-fs-benchmark.py" --out "$AGGREGATE_RUN_CSV" "$RUN_CSV"
    echo "wrote aggregate $AGGREGATE_RUN_CSV" >&2
    SUMMARY_CSV="$AGGREGATE_RUN_CSV"
    COMPARE_CSV="$AGGREGATE_RUN_CSV"
fi

if [[ -n "$ENV_JSON" ]]; then
    mkdir -p "$(dirname "$ENV_JSON")"
    cp "$ENV_JSON_RUN" "$ENV_JSON"
    echo "wrote $ENV_JSON" >&2
fi

if [[ "$DECOMPOSE" == "1" && -n "$DECOMPOSE_CSV" ]]; then
    mkdir -p "$(dirname "$DECOMPOSE_CSV")"
    cp "$DECOMPOSE_RUN_CSV" "$DECOMPOSE_CSV"
    echo "wrote $DECOMPOSE_CSV" >&2
fi

"$PYTHON_BIN" "$ROOT_DIR/scripts/fs-bench-summary.py" "$SUMMARY_CSV" >&2 || true

if [[ -n "$BASELINE" ]]; then
    "$PYTHON_BIN" "$ROOT_DIR/scripts/compare-baseline.py" --baseline "$BASELINE" "$COMPARE_CSV"
fi
