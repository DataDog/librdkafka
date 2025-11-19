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
    export BENCH_MAX_THROUGHPUT_MESSAGES=10000
    export BENCH_CONTROLLED_RATES="1000,2000"
    export BENCH_CONTROLLED_DURATION=10
fi

# Configure benchmark output directory
export BENCH_OUTPUT_DIR="$OUTPUT_DIR"

# Use test-runner
export DYLD_LIBRARY_PATH=../src:../src-cpp:$DYLD_LIBRARY_PATH
./test-runner 0200 2>&1 | tee "$OUTPUT_DIR/benchmark_output.txt"

# Check if CSV files were created
echo
echo "Checking results..."
if [ -f "$OUTPUT_DIR/summary.csv" ]; then
    echo "Found summary.csv"
    CSV_COUNT=$(wc -l < "$OUTPUT_DIR/per_message_latencies.csv" 2>/dev/null || echo "1")
    CSV_COUNT=$((CSV_COUNT - 1))  # Subtract header
    echo "Found $CSV_COUNT sampled message latencies"
else
    echo "Warning: No summary.csv found"
fi

# Print summary
echo
echo "=========================================="
echo "Results Summary"
echo "=========================================="
python3 - <<EOF
import csv
import sys

try:
    with open('$OUTPUT_DIR/summary.csv', 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

        if not rows:
            print("No results found in summary.csv")
            sys.exit(1)

        # Print each benchmark mode
        for row in rows:
            mode = row['mode']
            if mode == 'max_throughput':
                print("MAX THROUGHPUT MODE:")
            else:
                print(f"\nCONTROLLED RATE MODE ({row['rate_msgs_sec']} msgs/sec):")

            print(f"  Throughput:      {float(row['throughput_msgs_sec']):>10.1f} msg/sec")
            print(f"                   {float(row['throughput_mb_sec']):>10.2f} MB/sec")
            print(f"  Requests:        {float(row['requests_sent']):>10.0f} per run")
            print(f"  Batch Efficiency:{float(row['msgs_per_req']):>10.1f} msg/req")
            print(f"  Latency P50:     {float(row['latency_p50_ms']):>10.2f} ms")
            print(f"  Latency P95:     {float(row['latency_p95_ms']):>10.2f} ms")
            print(f"  Latency P99:     {float(row['latency_p99_ms']):>10.2f} ms")

            if int(row.get('messages_failed', 0)) > 0:
                print(f"  [WARNING] Failed:  {row['messages_failed']} messages")

except FileNotFoundError:
    print("No summary.csv found")
except Exception as e:
    print(f"Could not parse results: {e}")
    import traceback
    traceback.print_exc()
EOF
echo "=========================================="

# Generate plots
echo
echo "Generating plots..."

# Activate venv if available
if [ -f "$REPO_ROOT/tests/venv/bin/activate" ]; then
    source "$REPO_ROOT/tests/venv/bin/activate"
fi

cd "$REPO_ROOT/benchmarks"
python3 - "$OUTPUT_DIR" <<'PYEOF'
import csv
import sys
import os

if len(sys.argv) < 2:
    print("No output directory specified")
    sys.exit(1)

output_dir = sys.argv[1]
csv_file = f"{output_dir}/summary.csv"

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')
except ImportError:
    print("matplotlib not installed - skipping plots")
    print("Install with: pip3 install matplotlib")
    sys.exit(0)

try:
    # Read summary CSV
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("No results found in summary.csv")
        sys.exit(1)

    # Separate max throughput and controlled rate results
    max_throughput = [r for r in rows if r['mode'] == 'max_throughput']
    controlled_rates = [r for r in rows if r['mode'] == 'controlled']

    # Create plots based on available data
    if max_throughput:
        # Single max throughput result
        r = max_throughput[0]

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('MultiBatch Benchmark - Max Throughput Mode', fontsize=16, fontweight='bold')

        # Throughput
        throughput = float(r['throughput_msgs_sec'])
        ax1.bar(['Throughput'], [throughput], color='steelblue')
        ax1.set_ylabel('Messages/sec')
        ax1.set_title('Throughput')
        ax1.text(0, throughput/2, f"{int(throughput):,}\nmsg/sec",
                 ha='center', va='center', fontsize=12, fontweight='bold', color='white')

        # Efficiency
        msgs_per_req = float(r['msgs_per_req'])
        ax2.bar(['Msgs/Req'], [msgs_per_req], color='mediumseagreen')
        ax2.set_ylabel('Messages per ProduceRequest')
        ax2.set_title('Batching Efficiency')
        ax2.text(0, msgs_per_req/2, f"{msgs_per_req:.1f}",
                 ha='center', va='center', fontsize=12, fontweight='bold', color='white')

        # Requests
        requests = float(r['requests_sent'])
        ax3.bar(['Requests'], [requests], color='coral')
        ax3.set_ylabel('ProduceRequests')
        ax3.set_title('Request Count')
        ax3.text(0, requests/2, f"{int(requests):,}",
                 ha='center', va='center', fontsize=12, fontweight='bold', color='white')

        # Latency percentiles
        percentiles = ['P50', 'P95', 'P99']
        latencies = [float(r['latency_p50_ms']), float(r['latency_p95_ms']), float(r['latency_p99_ms'])]
        colors = ['lightblue', 'steelblue', 'darkblue']
        bars = ax4.bar(percentiles, latencies, color=colors)
        ax4.set_ylabel('Latency (ms)')
        ax4.set_title('Latency Percentiles')
        ax4.grid(axis='y', alpha=0.3)

        for bar, val in zip(bars, latencies):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{val:.2f}ms',
                    ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/benchmark_max_throughput.png', dpi=150)
        print(f"Saved: {output_dir}/benchmark_max_throughput.png")

    if controlled_rates:
        # Controlled rate comparison
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('MultiBatch Benchmark - Controlled Rate Modes', fontsize=16, fontweight='bold')

        rates = [int(r['rate_msgs_sec']) for r in controlled_rates]
        throughputs = [float(r['throughput_msgs_sec']) for r in controlled_rates]
        requests = [float(r['requests_sent']) for r in controlled_rates]
        msgs_per_req = [float(r['msgs_per_req']) for r in controlled_rates]
        p99_latencies = [float(r['latency_p99_ms']) for r in controlled_rates]

        # Throughput vs target
        ax1.plot(rates, throughputs, marker='o', color='steelblue', linewidth=2, markersize=8)
        ax1.plot(rates, rates, '--', color='gray', alpha=0.5, label='Target')
        ax1.set_xlabel('Target Rate (msgs/sec)')
        ax1.set_ylabel('Actual Throughput (msgs/sec)')
        ax1.set_title('Throughput vs Target Rate')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # Request count
        ax2.bar(range(len(rates)), requests, color='coral')
        ax2.set_xlabel('Test')
        ax2.set_ylabel('ProduceRequests')
        ax2.set_title('Request Count per Test')
        ax2.set_xticks(range(len(rates)))
        ax2.set_xticklabels([f'{r}' for r in rates], rotation=45)
        ax2.grid(axis='y', alpha=0.3)

        # Batching efficiency
        ax3.bar(range(len(rates)), msgs_per_req, color='mediumseagreen')
        ax3.set_xlabel('Test')
        ax3.set_ylabel('Messages per Request')
        ax3.set_title('Batching Efficiency')
        ax3.set_xticks(range(len(rates)))
        ax3.set_xticklabels([f'{r}' for r in rates], rotation=45)
        ax3.grid(axis='y', alpha=0.3)

        # P99 latency
        ax4.plot(rates, p99_latencies, marker='o', color='darkred', linewidth=2, markersize=8)
        ax4.set_xlabel('Rate (msgs/sec)')
        ax4.set_ylabel('P99 Latency (ms)')
        ax4.set_title('P99 Latency vs Rate')
        ax4.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(f'{output_dir}/benchmark_controlled_rates.png', dpi=150)
        print(f"Saved: {output_dir}/benchmark_controlled_rates.png")

except ImportError:
    print("matplotlib not installed - skipping plots")
    print("Install with: pip3 install matplotlib")
except FileNotFoundError:
    print("No summary.csv found - skipping plots")
except Exception as e:
    print(f"Error generating plots: {e}")
    import traceback
    traceback.print_exc()

PYEOF

# Deactivate venv
if [ -f "$REPO_ROOT/tests/venv/bin/activate" ]; then
    deactivate 2>/dev/null || true
fi

echo
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo "Full output:        $OUTPUT_DIR/benchmark_output.txt"
echo "Summary CSV:        $OUTPUT_DIR/summary.csv"
echo "Per-message CSV:    $OUTPUT_DIR/per_message_latencies.csv"
if [ -f "$OUTPUT_DIR/benchmark_max_throughput.png" ]; then
    echo "Max throughput plot: $OUTPUT_DIR/benchmark_max_throughput.png"
fi
if [ -f "$OUTPUT_DIR/benchmark_controlled_rates.png" ]; then
    echo "Controlled rates plot: $OUTPUT_DIR/benchmark_controlled_rates.png"
fi
echo "=========================================="
