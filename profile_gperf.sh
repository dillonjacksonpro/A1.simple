#!/bin/bash
# Profiling script: Google Performance Tools (gperftools) CPU profiler
# Uses named arguments matching the main binary

set -e  # Exit on error

# Parse named arguments
INPUT_FILE=""
OUTPUT_FILE=""
TIMING_FILE=""
NUM_THREADS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --input|-i)
            INPUT_FILE="$2"
            shift 2
            ;;
        --output|-o)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --timing|-t)
            TIMING_FILE="$2"
            shift 2
            ;;
        --threads|-n)
            NUM_THREADS="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 --input <path> [options]"
            echo ""
            echo "Required arguments:"
            echo "  --input, -i <path>         Input CSV file path"
            echo ""
            echo "Optional arguments:"
            echo "  --output, -o <path>        Output CSV file path (default: output.csv)"
            echo "  --timing, -t <path>        Timing report file path (default: timing.txt)"
            echo "  --threads, -n <count>      Number of threads to use (default: cpus * 2 - 1)"
            echo "  --help, -h                 Show this help message"
            echo ""
            echo "Example:"
            echo "  $0 --input data.csv --output results.csv --threads 8"
            exit 0
            ;;
        *)
            echo "Error: Unknown argument: $1"
            exit 1
            ;;
    esac
done

# Validate required argument
if [ -z "$INPUT_FILE" ]; then
    echo "Error: --input (or -i) is required"
    echo "Use --help for usage information"
    exit 1
fi

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

# Build command with arguments
CMD=("./$PROFILE_BINARY" --input "$INPUT_FILE")
[ -n "$OUTPUT_FILE" ] && CMD+=(--output "$OUTPUT_FILE")
[ -n "$TIMING_FILE" ] && CMD+=(--timing "$TIMING_FILE")
[ -n "$NUM_THREADS" ] && CMD+=(--threads "$NUM_THREADS")

echo "Running: ${CMD[@]}"
"${CMD[@]}"

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
