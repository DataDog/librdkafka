#!/bin/bash
#
# Run MultiBatch Benchmark Across Different Commits
#
# Usage: ./run_comparison.sh [options]
#
# Options:
#   --commits "commit1,commit2,..."  Commits to compare (default: current)
#   --names "name1,name2,..."        Names for each commit (default: short SHA)
#   --output DIR                     Output directory (default: ./results)
#   --quick                         Run in quick mode
#

set -e

# Defaults
COMMITS=""
NAMES=""
OUTPUT_DIR="./results"
QUICK_MODE=""
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --commits)
            COMMITS="$2"
            shift 2
            ;;
        --names)
            NAMES="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --quick)
            QUICK_MODE="1"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# If no commits specified, use current HEAD
if [ -z "$COMMITS" ]; then
    COMMITS="HEAD"
fi

# Convert comma-separated to array
IFS=',' read -ra COMMIT_ARRAY <<< "$COMMITS"
IFS=',' read -ra NAME_ARRAY <<< "$NAMES"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Save current branch/commit
ORIGINAL_REF=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || git rev-parse HEAD)

echo "=========================================="
echo "MultiBatch Benchmark Comparison"
echo "=========================================="
echo "Repository: $REPO_ROOT"
echo "Output: $OUTPUT_DIR"
echo "Commits: ${COMMIT_ARRAY[*]}"
echo "=========================================="
echo

# Function to build and run benchmark
run_benchmark() {
    local commit=$1
    local name=$2
    local output_file=$3

    echo "--- Benchmarking: $name (commit: $commit) ---"

    # Checkout commit
    echo "Checking out $commit..."
    git checkout -q "$commit"

    # Build
    echo "Building..."
    cd "$REPO_ROOT"
    if [ ! -f "configure" ]; then
        ./configure --enable-devel 2>&1 | tail -5
    fi
    make -j$(nproc 2>/dev/null || echo 4) 2>&1 | grep -E "(Error|Warning|Linking)" || true

    # Run benchmark
    echo "Running benchmark..."
    cd tests

    # Build test if needed
    make 0200-multibatch_benchmark 2>&1 | grep -E "(Error|Warning)" || true

    # Run with test framework
    if [ -n "$QUICK_MODE" ]; then
        export TEST_QUICK=1
    fi

    # Run test and capture output
    ./0200-multibatch_benchmark 2>&1 | tee "$output_file"

    echo "Results saved to $output_file"
    echo
}

# Cleanup function
cleanup() {
    echo "Restoring original ref: $ORIGINAL_REF"
    git checkout -q "$ORIGINAL_REF"
}

trap cleanup EXIT

# Run benchmark for each commit
for i in "${!COMMIT_ARRAY[@]}"; do
    commit="${COMMIT_ARRAY[$i]}"

    # Determine name
    if [ -n "${NAME_ARRAY[$i]}" ]; then
        name="${NAME_ARRAY[$i]}"
    else
        # Use short commit SHA as name
        name=$(git rev-parse --short "$commit")
    fi

    output_file="$OUTPUT_DIR/${name}.txt"

    run_benchmark "$commit" "$name" "$output_file"
done

# Extract JSON from all results
echo "=========================================="
echo "Extracting JSON results..."
echo "=========================================="

for i in "${!COMMIT_ARRAY[@]}"; do
    if [ -n "${NAME_ARRAY[$i]}" ]; then
        name="${NAME_ARRAY[$i]}"
    else
        name=$(git rev-parse --short "${COMMIT_ARRAY[$i]}")
    fi

    output_file="$OUTPUT_DIR/${name}.txt"
    json_file="$OUTPUT_DIR/${name}.json"

    # Extract JSON from test output (lines starting with '##')
    grep "^##" "$output_file" | sed 's/^## //' > "$json_file" 2>/dev/null || echo '{}' > "$json_file"

    echo "JSON: $json_file"
done

echo
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo "Results directory: $OUTPUT_DIR"
echo
echo "To compare results, run:"
echo "  python3 benchmarks/compare_results.py --results $OUTPUT_DIR/*.json"
echo
