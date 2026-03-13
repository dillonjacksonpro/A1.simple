#!/bin/bash
# Build script: Compile main.cc with conservative optimizations

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building main with conservative optimizations..."

# Conservative optimization flags:
# -O2: Good balance of speed and compile time, safer than O3
# -g0: Strip debug symbols for smaller binary
g++ \
    -std=c++17 \
    -O2 \
    -g0 \
    -Wall -Wextra -Wpedantic \
    -Wshadow -Wconversion -Wsign-conversion \
    -Wnull-dereference -Wdouble-promotion -Wformat=2 \
    -Werror \
    -fopenmp \
    main.cc \
    -o main

echo "✓ Build successful: ./main"
echo ""
echo "Usage: ./main <input_file> [output_file] [timing_file]"
