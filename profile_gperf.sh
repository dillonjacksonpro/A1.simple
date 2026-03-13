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
PERF_REPORT="perf_report.txt"

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

# Check if perf is available (optional, for cache/page fault analysis)
PERF_AVAILABLE=false
if command -v perf &> /dev/null; then
    PERF_AVAILABLE=true
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
if [ "$PERF_AVAILABLE" = true ]; then
    echo "Cache/page fault metrics will be written to: $PERF_REPORT"
fi
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
if [ "$PERF_AVAILABLE" = true ]; then
    echo ""
    perf stat \
        -e page-faults,minor-faults,major-faults \
        -e L1-dcache-loads,L1-dcache-load-misses \
        -e LLC-loads,LLC-load-misses \
        -e dTLB-loads,dTLB-load-misses \
        -e branch-instructions,branch-misses \
        -e cycles,instructions \
        -o "$PERF_REPORT" \
        "${CMD[@]}"
else
    "${CMD[@]}"
fi

unset CPUPROFILE
unset CPUPROFILE_FREQUENCY

echo ""
echo "✓ Profiling complete"
echo ""

# Display perf report if available
if [ "$PERF_AVAILABLE" = true ] && [ -f "$PERF_REPORT" ]; then
    echo "=== Cache & Memory Performance Report ==="
    echo ""
    cat "$PERF_REPORT"

    # Calculate and display derived metrics
    echo ""
    echo "=== Derived Metrics ==="
    echo ""

    L1_MISS_RATE=$(grep "L1-dcache-load-misses" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    L1_LOADS=$(grep "L1-dcache-loads" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    LLC_MISS_RATE=$(grep "LLC-load-misses" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    LLC_LOADS=$(grep "LLC-loads" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    PAGE_FAULTS=$(grep " page-faults" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    MAJOR_FAULTS=$(grep " major-faults" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    MINOR_FAULTS=$(grep " minor-faults" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    BRANCH_MISSES=$(grep " branch-misses" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    BRANCH_INSTRUCTIONS=$(grep " branch-instructions" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')
    INSTRUCTIONS=$(grep " instructions" "$PERF_REPORT" | tail -1 | awk '{print $1}' | tr -d ',')
    CYCLES=$(grep " cycles" "$PERF_REPORT" | head -1 | awk '{print $1}' | tr -d ',')

    [ -n "$PAGE_FAULTS" ] && echo "Total page faults:       $PAGE_FAULTS"
    [ -n "$MAJOR_FAULTS" ] && echo "Major page faults (I/O): $MAJOR_FAULTS"
    [ -n "$MINOR_FAULTS" ] && echo "Minor page faults:       $MINOR_FAULTS"

    if [ -n "$L1_LOADS" ] && [ "$L1_LOADS" -gt 0 ]; then
        L1_MISS_PCT=$(awk "BEGIN {printf \"%.2f\", ($L1_MISS_RATE / $L1_LOADS) * 100}")
        echo "L1 cache miss rate:      $L1_MISS_PCT%"
    fi

    if [ -n "$LLC_LOADS" ] && [ "$LLC_LOADS" -gt 0 ]; then
        LLC_MISS_PCT=$(awk "BEGIN {printf \"%.2f\", ($LLC_MISS_RATE / $LLC_LOADS) * 100}")
        echo "L3 cache miss rate:      $LLC_MISS_PCT%"
    fi

    if [ -n "$BRANCH_INSTRUCTIONS" ] && [ "$BRANCH_INSTRUCTIONS" -gt 0 ]; then
        BRANCH_MISS_PCT=$(awk "BEGIN {printf \"%.2f\", ($BRANCH_MISSES / $BRANCH_INSTRUCTIONS) * 100}")
        echo "Branch miss rate:        $BRANCH_MISS_PCT%"
    fi

    if [ -n "$INSTRUCTIONS" ] && [ "$CYCLES" -gt 0 ]; then
        IPC=$(awk "BEGIN {printf \"%.3f\", $INSTRUCTIONS / $CYCLES}")
        echo "Instructions per cycle:  $IPC"
    fi
    echo ""
fi

# Check if pprof is available
if ! command -v pprof &> /dev/null; then
    echo "Warning: pprof not found (part of google-perftools)"
    echo "Install with: sudo apt-get install google-perftools"
    echo "Profile data saved to: $PROFILE_DATA"
    echo "View with: pprof --text_report $PROFILE_BINARY $PROFILE_DATA"
    exit 0
fi

echo "=== CPU Profile Report (Top Functions) ==="
echo ""
pprof --text "$PROFILE_BINARY" "$PROFILE_DATA" | head -30

echo ""
echo "=== Analysis & Further Reports ==="
echo ""
echo "Detailed CPU profile (top 30):"
echo "  pprof --text $PROFILE_BINARY $PROFILE_DATA | head -30"
echo ""
echo "Flame graph (requires graphviz):"
echo "  pprof --svg $PROFILE_BINARY $PROFILE_DATA > profile.svg"
echo ""
echo "Web-based interactive graph (requires dot):"
echo "  pprof --web $PROFILE_BINARY $PROFILE_DATA"
echo ""
echo "Function-specific analysis:"
echo "  pprof --list=<function_name> $PROFILE_BINARY $PROFILE_DATA"
echo ""
echo "Files generated:"
echo "  CPU profile data:  $PROFILE_DATA"
echo "  Memory metrics:    $PERF_REPORT"
echo "  Binary:            $PROFILE_BINARY"
