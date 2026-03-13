#!/bin/bash
# Profiling script: Compile for profiling and run with perf

set -e  # Exit on error

# Check arguments
if [ $# -lt 3 ]; then
    echo "Usage: $0 <binary_target> <input_file> <perf_frequency>"
    echo ""
    echo "Arguments:"
    echo "  binary_target      Path to binary to profile (e.g., ./main)"
    echo "  input_file         Input CSV file to process"
    echo "  perf_frequency     Sampling frequency (e.g., 99, 999, 4000)"
    echo ""
    echo "Example: $0 ./main data.csv 4000"
    exit 1
fi

BINARY_TARGET="$1"
INPUT_FILE="$2"
PERF_FREQ="$3"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BINARY_NAME=$(basename "$BINARY_TARGET")
PROFILE_BINARY="${BINARY_NAME}_profile"

# Verify input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file not found: $INPUT_FILE"
    exit 1
fi

echo "=== Profiling Build Phase ==="
echo "Compiling $BINARY_NAME with debug symbols (no optimization)..."

# Compile for profiling:
# -O0: No optimization (better profiling accuracy)
# -g: Debug symbols (needed for perf to map addresses to functions)
# -fno-omit-frame-pointer: Keep frame pointers (helps perf stack walking)
g++ \
    -std=c++17 \
    -O0 \
    -g \
    -fno-omit-frame-pointer \
    -Wall -Wextra -Wpedantic \
    -Wshadow -Wconversion -Wsign-conversion \
    -Wnull-dereference -Wdouble-promotion -Wformat=2 \
    -fopenmp \
    main.cc \
    -o "$PROFILE_BINARY"

echo "✓ Profile binary created: $PROFILE_BINARY"
echo ""

# Run profiling
echo "=== Perf Record Phase ==="
echo "Running: perf record -F $PERF_FREQ -g $PROFILE_BINARY $INPUT_FILE"
echo ""

perf record \
    -F "$PERF_FREQ" \
    -g \
    -o perf.data \
    "$PROFILE_BINARY" "$INPUT_FILE" output.csv timing.csv

echo ""
echo "✓ Performance data recorded to perf.data"
echo ""

# Run perf stat for summary statistics
echo "=== Perf Stat Summary ==="
perf stat "$PROFILE_BINARY" "$INPUT_FILE" output.csv timing.csv 2>&1
echo ""

# Generate profiling report with demangling
echo "=== Perf Report (Top Functions) ==="
echo "Generating call stack with C++ symbol demangling..."
echo ""

perf report -i perf.data --stdio \
    --symbols=/usr/bin/c++filt \
    -n \
    --no-children \
    -U | head -50

echo ""
echo "=== Profile Complete ==="
echo ""
echo "Full report: perf report -i perf.data"
echo "Flamegraph (if installed): perf script | stackcollapse-perf.pl | flamegraph.pl"
echo "Profile binary: $PROFILE_BINARY"
echo "Performance data: perf.data"
