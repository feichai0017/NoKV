#!/bin/bash

# This script automates the analysis of Go pprof profiles (CPU and Memory)
# and generates SVG visualizations (flame graphs and call graphs).

# Dependencies:
# - go tool pprof (comes with Go installation)
# - graphviz (for 'dot' command, needed for call graphs)
#   Install on Ubuntu/Debian: sudo apt-get install graphviz
#   Install on macOS: brew install graphviz

OUTPUT_DIR="pprof_output"
CPU_PROF="cpu.prof"
MEM_PROF="mem.prof"

echo "Starting pprof analysis..."

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"
echo "Output will be saved in: $OUTPUT_DIR"

# --- Analyze CPU Profile ---
if [ -f "$CPU_PROF" ]; then
    echo "Analyzing CPU profile: $CPU_PROF"
    echo "Generating CPU flame graph..."
    go tool pprof -svg "$CPU_PROF" > "$OUTPUT_DIR/cpu_flame.svg"
    if [ $? -eq 0 ]; then
        echo "CPU flame graph generated: $OUTPUT_DIR/cpu_flame.svg"
    else
        echo "Error generating CPU flame graph."
    fi

    echo "Generating CPU call graph..."
    if command -v dot &> /dev/null; then
        go tool pprof -dot "$CPU_PROF" | dot -Tsvg > "$OUTPUT_DIR/cpu_call.svg"
        if [ $? -eq 0 ]; then
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
    go tool pprof -alloc_space -svg "$MEM_PROF" > "$OUTPUT_DIR/mem_alloc_flame.svg"
    if [ $? -eq 0 ]; then
        echo "Memory alloc_space flame graph generated: $OUTPUT_DIR/mem_alloc_flame.svg"
    else
        echo "Error generating Memory alloc_space flame graph."
    fi

    echo "Generating Memory call graph (alloc_space)..."
    if command -v dot &> /dev/null; then
        go tool pprof -alloc_space -dot "$MEM_PROF" | dot -Tsvg > "$OUTPUT_DIR/mem_alloc_call.svg"
        if [ $? -eq 0 ]; then
            echo "Memory alloc_space call graph generated: $OUTPUT_DIR/mem_alloc_call.svg"
        else
            echo "Error generating Memory alloc_space call graph."
        fi
    else
        echo "Graphviz 'dot' command not found. Skipping Memory call graph generation."
    fi

    echo "Generating Memory flame graph (inuse_space)..."
    go tool pprof -inuse_space -svg "$MEM_PROF" > "$OUTPUT_DIR/mem_inuse_flame.svg"
    if [ $? -eq 0 ]; then
        echo "Memory inuse_space flame graph generated: $OUTPUT_DIR/mem_inuse_flame.svg"
    else
        echo "Error generating Memory inuse_space flame graph."
    fi

    echo "Generating Memory call graph (inuse_space)..."
    if command -v dot &> /dev/null; then
        go tool pprof -inuse_space -dot "$MEM_PROF" | dot -Tsvg > "$OUTPUT_DIR/mem_inuse_call.svg"
        if [ $? -eq 0 ]; then
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
