#!/bin/bash
# Profiling script: Google Performance Tools (gperftools) CPU profiler

set -e  # Exit on error

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_file> [num_threads]"
    echo ""
    echo "Arguments:"
    echo "  input_file      Input CSV file to process"
    echo "  num_threads     Optional: number of threads to use"
    echo ""
    echo "Example: $0 data.csv 8"
    exit 1
fi

INPUT_FILE="$1"
NUM_THREADS="${2:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PROFILE_BINARY="main_gperf"
PROFILE_DATA="main.prof"

# Verify input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file not found: $INPUT_FILE"
    exit 1
fi

# Check if gperftools is installed
if ! pkg-config --exists libprofiler 2>/dev/null; then
    echo "Error: gperftools (libprofiler) not found"
    echo "Install with: sudo apt-get install google-perftools libgoogle-perftools-dev"
    exit 1
fi

GPERF_CFLAGS=$(pkg-config --cflags libprofiler)
GPERF_LIBS=$(pkg-config --libs libprofiler)

echo "=== Google Performance Tools CPU Profiling ==="
echo "Compiling with gperftools..."

# Compile for profiling with gperftools
# -g: Debug symbols (needed for pprof to map addresses to functions)
# -lprofiler: Link against gperftools profiler library
g++ \
    -std=c++17 \
    -O2 \
    -g \
    -Wall -Wextra -Wpedantic \
    -Wshadow -Wconversion -Wsign-conversion \
    -Wnull-dereference -Wdouble-promotion -Wformat=2 \
    -fopenmp \
    $GPERF_CFLAGS \
    main.cc \
    -o "$PROFILE_BINARY" \
    $GPERF_LIBS

echo "✓ Profile binary created: $PROFILE_BINARY"
echo ""

echo "=== Running with CPU Profiling ==="
echo "Profile data will be written to: $PROFILE_DATA"
echo ""

# Run with CPU profiling enabled
# CPUPROFILE: Output file for profiling data
# CPUPROFILE_FREQUENCY: Sampling frequency (Hz) - default is 100
export CPUPROFILE="$PROFILE_DATA"
export CPUPROFILE_FREQUENCY=100

if [ -n "$NUM_THREADS" ]; then
    echo "Running: $PROFILE_BINARY $INPUT_FILE output.csv timing.csv $NUM_THREADS"
    ./"$PROFILE_BINARY" "$INPUT_FILE" output.csv timing.csv "$NUM_THREADS"
else
    echo "Running: $PROFILE_BINARY $INPUT_FILE output.csv timing.csv"
    ./"$PROFILE_BINARY" "$INPUT_FILE" output.csv timing.csv
fi

unset CPUPROFILE
unset CPUPROFILE_FREQUENCY

echo ""
echo "✓ Profiling complete"
echo ""

# Check if pprof is available
if ! command -v pprof &> /dev/null; then
    echo "Warning: pprof not found (part of google-perftools)"
    echo "Install with: sudo apt-get install google-perftools"
    echo "Profile data saved to: $PROFILE_DATA"
    echo "View with: pprof --text_report $PROFILE_BINARY $PROFILE_DATA"
    exit 0
fi

echo "=== Profile Report (Text) ==="
echo ""
pprof --text "$PROFILE_BINARY" "$PROFILE_DATA" | head -40

echo ""
echo "=== Profile Report (Top Functions by CPU Time) ==="
echo ""
pprof --text "$PROFILE_BINARY" "$PROFILE_DATA" | grep -E "^\s+[0-9]" | head -20

echo ""
echo "=== Additional Analysis ==="
echo ""
echo "Generate detailed reports:"
echo ""
echo "Text report (top 30):"
echo "  pprof --text $PROFILE_BINARY $PROFILE_DATA | head -30"
echo ""
echo "Flame graph (requires graphviz):"
echo "  pprof --svg $PROFILE_BINARY $PROFILE_DATA > profile.svg"
echo ""
echo "Web-based interactive graph (requires dot):"
echo "  pprof --web $PROFILE_BINARY $PROFILE_DATA"
echo ""
echo "Function list:"
echo "  pprof --list=<function_name> $PROFILE_BINARY $PROFILE_DATA"
echo ""
echo "Profile data: $PROFILE_DATA"
echo "Binary: $PROFILE_BINARY"
