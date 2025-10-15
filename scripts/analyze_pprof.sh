#!/usr/bin/env bash
set -euo pipefail

# This script generates CPU and memory pprof profiles from Go benchmarks,
# then produces SVG call graphs (Graphviz) and SVG flame graphs (Brendan Gregg's FlameGraph).

# Requirements:
# - Go toolchain (for `go`, `go tool pprof`)
# - Graphviz (`dot`) for call graphs  [optional but recommended]
# - FlameGraph scripts for flame graphs:
#     git clone https://github.com/brendangregg/FlameGraph ~/FlameGraph
#     export FLAMEGRAPH_DIR=~/FlameGraph

# === Config ===
PKG="./benchmark"                 # Go package that contains your benchmarks
BINARY="bench.test"               # Compiled test binary name
OUTPUT_DIR="pprof_output"
CPU_PROF="cpu.prof"
MEM_PROF="mem.prof"
GRAPH_DPI="${GRAPH_DPI:-200}"

# FlameGraph location (override with: export FLAMEGRAPH_DIR=/path/to/FlameGraph)
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-$HOME/FlameGraph}"
STACK_COLLAPSE="$FLAMEGRAPH_DIR/stackcollapse-go.pl"
FLAMEGRAPH_PL="$FLAMEGRAPH_DIR/flamegraph.pl"

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1"; exit 1; }; }

# Mandatory dependencies
need go
need awk

# Optional dependency (for call graphs)
if ! command -v dot >/dev/null 2>&1; then
  echo "Graphviz 'dot' not found. Call graph generation will be skipped."
  DOT_MISSING=1
else
  DOT_MISSING=0
fi

# FlameGraph scripts check
if [[ ! -x "$STACK_COLLAPSE" || ! -x "$FLAMEGRAPH_PL" ]]; then
  echo "FlameGraph scripts not found:"
  echo "  $STACK_COLLAPSE"
  echo "  $FLAMEGRAPH_PL"
  echo "Please install FlameGraph (e.g., git clone https://github.com/brendangregg/FlameGraph \"${FLAMEGRAPH_DIR}\")"
  echo "or set FLAMEGRAPH_DIR to the correct location."
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "== Build benchmark test binary =="
go test -c -o "$BINARY" "$PKG"

echo "== Run benchmarks and collect profiles =="
./"$BINARY" -test.run=^$ -test.bench=. -test.cpuprofile="$CPU_PROF" -test.memprofile="$MEM_PROF"

[[ -f "$CPU_PROF" ]] && echo "CPU profile created: $CPU_PROF"
[[ -f "$MEM_PROF" ]] && echo "Memory profile created: $MEM_PROF"
echo

# --- Call graph (Graphviz) ---
gen_call_graph() {
  local prof="$1" out="$2" extra_flag="${3:-}"
  if (( DOT_MISSING )); then
    echo "Skipping call graph (Graphviz not installed)."
    return
  fi
  echo "Generating call graph: $out"
  go tool pprof -dot $extra_flag "./$BINARY" "$prof" | dot -Tsvg -Gdpi="$GRAPH_DPI" > "$out"
}

# --- Flame graph (via FlameGraph) ---
# Note: `go tool pprof -svg` produces a call graph, not a flame graph.
# We export raw stacks, collapse them, then render with flamegraph.pl.
gen_flame_from_pprof() {
  local prof="$1" out="$2" title="$3" raw_flag="${4:-}"
  local tmp_txt
  tmp_txt="$(mktemp)"
  echo "Generating flame graph: $out ($title)"
  # 1) Export raw stacks from pprof
  go tool pprof -raw $raw_flag "./$BINARY" "$prof" > "$tmp_txt"
  # 2) Collapse Go stacks to folded format
  "$STACK_COLLAPSE" "$tmp_txt" | \
  # 3) Render SVG flame graph
  "$FLAMEGRAPH_PL" --title "$title" --width 1600 > "$out"
  rm -f "$tmp_txt"
}

# === CPU ===
if [[ -f "$CPU_PROF" ]]; then
  gen_call_graph "$CPU_PROF" "$OUTPUT_DIR/cpu_call.svg"
  gen_flame_from_pprof "$CPU_PROF" "$OUTPUT_DIR/cpu_flame.svg" "CPU Flame Graph" ""
else
  echo "CPU profile '$CPU_PROF' not found. Skipping CPU analysis."
fi
echo

# === Memory (alloc_space / inuse_space) ===
if [[ -f "$MEM_PROF" ]]; then
  gen_call_graph "$MEM_PROF" "$OUTPUT_DIR/mem_alloc_call.svg" "-alloc_space"
  gen_call_graph "$MEM_PROF" "$OUTPUT_DIR/mem_inuse_call.svg" "-inuse_space"

  gen_flame_from_pprof "$MEM_PROF" "$OUTPUT_DIR/mem_alloc_flame.svg" "Heap Flame (alloc_space)" "-alloc_space"
  gen_flame_from_pprof "$MEM_PROF" "$OUTPUT_DIR/mem_inuse_flame.svg" "Heap Flame (inuse_space)" "-inuse_space"
else
  echo "Memory profile '$MEM_PROF' not found. Skipping memory analysis."
fi

echo
echo "Done. SVG outputs are in: $OUTPUT_DIR"
echo "Tip (interactive flame graph): go tool pprof -http=:0 ./$BINARY $CPU_PROF"
