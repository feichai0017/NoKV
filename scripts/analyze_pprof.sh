#!/bin/bash

# This script automates the generation and analysis of Go pprof profiles (CPU and Memory)
# and generates SVG visualizations (flame graphs and call graphs).

# Dependencies:
# - go tool pprof (comes with Go installation)
# - graphviz (for 'dot' command, needed for call graphs)
#   Install on Ubuntu/Debian: sudo apt-get install graphviz
#   Install on macOS: brew install graphviz

OUTPUT_DIR="pprof_output"
CPU_PROF="cpu.prof"
MEM_PROF="mem.prof"
GRAPH_DPI="${GRAPH_DPI:-200}"

generate_svg() {
    local output_path="$1"
    shift
    local tmp_dot

    if command -v dot >/dev/null 2>&1; then
        tmp_dot=$(mktemp)
        if ! go tool pprof -dot "$@" > "$tmp_dot"; then
            rm -f "$tmp_dot"
            return 1
        fi
        if ! dot -Tsvg -Gdpi="$GRAPH_DPI" "$tmp_dot" > "$output_path"; then
            rm -f "$tmp_dot"
            return 1
        fi
        rm -f "$tmp_dot"
        return 0
    fi

    go tool pprof -svg "$@" > "$output_path"
}

echo "Starting pprof generation and analysis..."

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"
echo "Output will be saved in: $OUTPUT_DIR"

# --- Generate Profiles ---
echo "Generating CPU and Memory profiles by running benchmarks..."
go test -bench=. -cpuprofile="$CPU_PROF" -memprofile="$MEM_PROF" ./benchmark
if [ $? -eq 0 ]; then
    echo "Profiles generated successfully: $CPU_PROF, $MEM_PROF"
else
    echo "Error generating profiles. Aborting analysis."
    exit 1
fi

echo ""

# --- Analyze CPU Profile ---
if [ -f "$CPU_PROF" ]; then
    echo "Analyzing CPU profile: $CPU_PROF"
    echo "Generating CPU flame graph..."
    if generate_svg "$OUTPUT_DIR/cpu_flame.svg" "$CPU_PROF"; then
        echo "CPU flame graph generated: $OUTPUT_DIR/cpu_flame.svg"
    else
        echo "Error generating CPU flame graph."
    fi

    echo "Generating CPU call graph..."
    if command -v dot >/dev/null 2>&1; then
        if generate_svg "$OUTPUT_DIR/cpu_call.svg" "$CPU_PROF"; then
            echo "CPU call graph generated: $OUTPUT_DIR/cpu_call.svg"
        else
            echo "Error generating CPU call graph."
        fi
    else
        echo "Graphviz 'dot' command not found. Skipping CPU call graph generation."
        echo "Please install Graphviz (e.g., 'sudo apt-get install graphviz' or 'brew install graphviz') to generate call graphs."
    fi
else
    echo "CPU profile '$CPU_PROF' not found. Skipping CPU analysis."
fi

echo ""

# --- Analyze Memory Profile ---
if [ -f "$MEM_PROF" ]; then
    echo "Analyzing Memory profile: $MEM_PROF"
    echo "Generating Memory flame graph (alloc_space)..."
    if generate_svg "$OUTPUT_DIR/mem_alloc_flame.svg" -alloc_space "$MEM_PROF"; then
        echo "Memory alloc_space flame graph generated: $OUTPUT_DIR/mem_alloc_flame.svg"
    else
        echo "Error generating Memory alloc_space flame graph."
    fi

    echo "Generating Memory call graph (alloc_space)..."
    if command -v dot >/dev/null 2>&1; then
        if generate_svg "$OUTPUT_DIR/mem_alloc_call.svg" -alloc_space "$MEM_PROF"; then
            echo "Memory alloc_space call graph generated: $OUTPUT_DIR/mem_alloc_call.svg"
        else
            echo "Error generating Memory alloc_space call graph."
        fi
    else
        echo "Graphviz 'dot' command not found. Skipping Memory call graph generation."
    fi

    echo "Generating Memory flame graph (inuse_space)..."
    if generate_svg "$OUTPUT_DIR/mem_inuse_flame.svg" -inuse_space "$MEM_PROF"; then
        echo "Memory inuse_space flame graph generated: $OUTPUT_DIR/mem_inuse_flame.svg"
    else
        echo "Error generating Memory inuse_space flame graph."
    fi

    echo "Generating Memory call graph (inuse_space)..."
    if command -v dot >/dev/null 2>&1; then
        if generate_svg "$OUTPUT_DIR/mem_inuse_call.svg" -inuse_space "$MEM_PROF"; then
            echo "Memory inuse_space call graph generated: $OUTPUT_DIR/mem_inuse_call.svg"
        else
            echo "Error generating Memory inuse_space call graph."
        fi
    else
        echo "Graphviz 'dot' command not found. Skipping Memory call graph generation."
    fi

else
    echo "Memory profile '$MEM_PROF' not found. Skipping Memory analysis."
fi

echo ""
echo "Analysis complete. Open the .svg files in your web browser to view the graphs."
echo "Example: open $OUTPUT_DIR/cpu_flame.svg"
