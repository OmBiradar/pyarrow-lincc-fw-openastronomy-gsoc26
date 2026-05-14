#!/bin/bash

# Fail fast: exit immediately if any command fails, or if an undefined variable is used
set -euo pipefail

# Print commands as they are executed (CI debugging)
set -x

# Absolute paths based on the GitHub Actions workspace
ROOT_DIR=$(pwd)
ARROW_CPP_DIR="${ROOT_DIR}/arrow-src/cpp"
BUILD_DIR="${ROOT_DIR}/build"

echo "========================================="
echo " Configuring Apache Arrow C++ Build      "
echo "========================================="

if [ ! -d "$ARROW_CPP_DIR" ]; then
    echo "Error: Could not find $ARROW_CPP_DIR."
    echo "Check that the actions/checkout step successfully pulled the Arrow repository."
    exit 1
fi

mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Run CMake with optimizations for CI and benchmarking
cmake "${ARROW_CPP_DIR}" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DARROW_USE_CCACHE=ON \
  -DARROW_BUILD_BENCHMARKS=ON \
  -DARROW_COMPUTE=ON \
  -DARROW_DATASET=ON \
  -DARROW_PARQUET=ON

echo "========================================="
echo " Compiling Apache Arrow                  "
echo "========================================="

# Build using Ninja
ninja