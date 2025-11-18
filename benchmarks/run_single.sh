#!/bin/bash
#
# Run benchmark on current code and generate visualizations
#
# Usage: ./run_single.sh [--quick] [--output DIR]
#

set -e

QUICK_MODE=""
OUTPUT_DIR="./benchmark_output"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE="1"
            shift
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        *)
            echo "Usage: $0 [--quick] [--output DIR]"
            exit 1
            ;;
    esac
done

# Make output dir absolute
OUTPUT_DIR="$(cd "$(dirname "$OUTPUT_DIR")" 2>/dev/null && pwd)/$(basename "$OUTPUT_DIR")" || OUTPUT_DIR="$(pwd)/$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

echo "=========================================="
echo "Running MultiBatch Benchmark"
echo "=========================================="
echo "Repository: $REPO_ROOT"
echo "Output: $OUTPUT_DIR"
if [ -n "$QUICK_MODE" ]; then
    echo "Mode: QUICK"
fi
echo "=========================================="
echo

# Build
cd "$REPO_ROOT/tests"
echo "Building benchmark..."
make build 2>&1 | tail -5

# Run
echo "Running benchmark..."
if [ -n "$QUICK_MODE" ]; then
    export TEST_QUICK=1
fi

# Use test-runner
export DYLD_LIBRARY_PATH=../src:../src-cpp:$DYLD_LIBRARY_PATH
./test-runner 0200 2>&1 | tee "$OUTPUT_DIR/benchmark_output.txt"

# Extract JSON
echo
echo "Extracting results..."
# Look for "Report #N: {json}" format from TEST_REPORT
grep "Report #" "$OUTPUT_DIR/benchmark_output.txt" | sed 's/.*Report #[0-9]*: //' > "$OUTPUT_DIR/results.json" 2>/dev/null || {
    echo "Warning: Could not extract JSON from output"
    echo '{}' > "$OUTPUT_DIR/results.json"
}

# Print summary
echo
echo "=========================================="
echo "Results Summary"
echo "=========================================="
python3 - <<EOF
import json
try:
    with open('$OUTPUT_DIR/results.json', 'r') as f:
        data = json.load(f)
        if 'results' in data:
            r = data['results']
            print(f"Throughput:      {r['throughput_msg_sec']:>10.1f} msg/sec")
            print(f"                 {r['throughput_mb_sec']:>10.2f} MB/sec")
            print(f"Requests:        {r['request_count_mean']:>10.1f} per run")
            print(f"Batch Efficiency:{r['messages_per_request']:>10.1f} msg/req")
            print(f"")
            print(f"Latency P50:     {r['latency_p50_ms']:>10.2f} ms")
            print(f"Latency P95:     {r['latency_p95_ms']:>10.2f} ms")
            print(f"Latency P99:     {r['latency_p99_ms']:>10.2f} ms")
            print(f"Latency P999:    {r['latency_p999_ms']:>10.2f} ms")
except Exception as e:
    print(f"Could not parse results: {e}")
EOF
echo "=========================================="

# Generate plots
echo
echo "Generating plots..."
cd "$REPO_ROOT/benchmarks"
python3 - "$OUTPUT_DIR" <<'PYEOF'
import json
import sys
import os

if len(sys.argv) < 2:
    print("No output directory specified")
    sys.exit(1)

output_dir = sys.argv[1]
json_file = f"{output_dir}/results.json"

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')
except ImportError:
    print("matplotlib not installed - skipping plots")
    print("Install with: pip3 install matplotlib")
    sys.exit(0)

try:
    with open(json_file, 'r') as f:
        data = json.load(f)

    if 'results' not in data:
        print("No results found in JSON")
        sys.exit(1)

    r = data['results']

    # Create a summary plot
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('MultiBatch Benchmark Results', fontsize=16, fontweight='bold')

    # Throughput
    ax1.bar(['Throughput'], [r['throughput_msg_sec']], color='steelblue')
    ax1.set_ylabel('Messages/sec')
    ax1.set_title('Throughput')
    ax1.text(0, r['throughput_msg_sec']/2, f"{int(r['throughput_msg_sec']):,}\nmsg/sec",
             ha='center', va='center', fontsize=12, fontweight='bold', color='white')

    # Efficiency
    ax2.bar(['Msgs/Req'], [r['messages_per_request']], color='mediumseagreen')
    ax2.set_ylabel('Messages per ProduceRequest')
    ax2.set_title('Batching Efficiency')
    ax2.text(0, r['messages_per_request']/2, f"{r['messages_per_request']:.1f}",
             ha='center', va='center', fontsize=12, fontweight='bold', color='white')

    # Requests
    ax3.bar(['Requests'], [r['request_count_mean']], color='coral')
    ax3.set_ylabel('ProduceRequests per run')
    ax3.set_title('Request Count')
    ax3.text(0, r['request_count_mean']/2, f"{int(r['request_count_mean']):,}",
             ha='center', va='center', fontsize=12, fontweight='bold', color='white')

    # Latency percentiles
    percentiles = ['P50', 'P95', 'P99', 'P999']
    latencies = [r['latency_p50_ms'], r['latency_p95_ms'],
                 r['latency_p99_ms'], r['latency_p999_ms']]
    colors = ['lightblue', 'steelblue', 'darkblue', 'navy']
    bars = ax4.bar(percentiles, latencies, color=colors)
    ax4.set_ylabel('Latency (ms)')
    ax4.set_title('Latency Percentiles')
    ax4.grid(axis='y', alpha=0.3)

    # Add value labels on latency bars
    for bar, val in zip(bars, latencies):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.2f}ms',
                ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/benchmark_summary.png', dpi=150)
    print(f"Saved: {output_dir}/benchmark_summary.png")

except ImportError:
    print("matplotlib not installed - skipping plots")
    print("Install with: pip3 install matplotlib")
except Exception as e:
    print(f"Error generating plots: {e}")

PYEOF

echo
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo "Full output: $OUTPUT_DIR/benchmark_output.txt"
echo "JSON results: $OUTPUT_DIR/results.json"
echo "Summary plot: $OUTPUT_DIR/benchmark_summary.png"
echo "=========================================="
