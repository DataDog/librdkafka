#!/usr/bin/env python3
"""
Compare MultiBatch Benchmark Results

Generates comparison tables and graphs from benchmark JSON outputs.
"""

import json
import sys
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend

def load_results(json_files):
    """Load benchmark results from JSON files"""
    results = []
    for json_file in json_files:
        with open(json_file, 'r') as f:
            data = json.load(f)
            data['name'] = Path(json_file).stem
            results.append(data)
    return results

def print_comparison_table(results):
    """Print comparison table to stdout"""
    print("\n" + "="*100)
    print("BENCHMARK COMPARISON")
    print("="*100)

    # Header
    header = f"{'Version':<20} {'Throughput':<20} {'Requests':<15} {'Msgs/Req':<12} {'P50':<8} {'P99':<8}"
    print(header)
    print("-"*100)

    # Rows
    for r in results:
        res = r['results']
        print(f"{r['name']:<20} "
              f"{res['throughput_msg_sec']:>8.0f} msg/sec   "
              f"{res['request_count_mean']:>8.1f}       "
              f"{res['messages_per_request']:>8.1f}    "
              f"{res['latency_p50_ms']:>6.2f}  "
              f"{res['latency_p99_ms']:>6.2f}")

    print("="*100)

    # Calculate improvements (if we have a baseline)
    if len(results) >= 2:
        baseline = results[0]
        print("\nIMPROVEMENTS (vs {})".format(baseline['name']))
        print("-"*100)

        for r in results[1:]:
            throughput_imp = r['results']['throughput_msg_sec'] / baseline['results']['throughput_msg_sec']
            request_reduction = baseline['results']['request_count_mean'] / r['results']['request_count_mean']
            p99_improvement = baseline['results']['latency_p99_ms'] / r['results']['latency_p99_ms']

            print(f"{r['name']:<20} "
                  f"Throughput: {throughput_imp:>5.2f}x   "
                  f"Request reduction: {request_reduction:>5.2f}x   "
                  f"P99 latency: {p99_improvement:>5.2f}x")

        print("="*100)

def plot_comparison(results, output_dir):
    """Generate comparison plots"""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    names = [r['name'] for r in results]

    # Throughput comparison
    fig, ax = plt.subplots(figsize=(10, 6))
    throughputs = [r['results']['throughput_msg_sec'] for r in results]
    bars = ax.bar(names, throughputs, color='steelblue')
    ax.set_ylabel('Messages/sec')
    ax.set_title('Throughput Comparison')
    ax.grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(output_dir / 'throughput_comparison.png', dpi=150)
    print(f"Saved: {output_dir / 'throughput_comparison.png'}")
    plt.close()

    # Request count comparison
    fig, ax = plt.subplots(figsize=(10, 6))
    requests = [r['results']['request_count_mean'] for r in results]
    bars = ax.bar(names, requests, color='coral')
    ax.set_ylabel('ProduceRequests per run')
    ax.set_title('Request Count Comparison (lower is better)')
    ax.grid(axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(output_dir / 'request_count_comparison.png', dpi=150)
    print(f"Saved: {output_dir / 'request_count_comparison.png'}")
    plt.close()

    # Messages per request (efficiency)
    fig, ax = plt.subplots(figsize=(10, 6))
    msgs_per_req = [r['results']['messages_per_request'] for r in results]
    bars = ax.bar(names, msgs_per_req, color='mediumseagreen')
    ax.set_ylabel('Messages per ProduceRequest')
    ax.set_title('Batching Efficiency (higher is better)')
    ax.grid(axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(output_dir / 'batching_efficiency.png', dpi=150)
    print(f"Saved: {output_dir / 'batching_efficiency.png'}")
    plt.close()

    # Latency comparison
    fig, ax = plt.subplots(figsize=(12, 6))
    x = range(len(names))
    width = 0.2

    p50s = [r['results']['latency_p50_ms'] for r in results]
    p95s = [r['results']['latency_p95_ms'] for r in results]
    p99s = [r['results']['latency_p99_ms'] for r in results]
    p999s = [r['results']['latency_p999_ms'] for r in results]

    ax.bar([i - 1.5*width for i in x], p50s, width, label='P50', color='lightblue')
    ax.bar([i - 0.5*width for i in x], p95s, width, label='P95', color='steelblue')
    ax.bar([i + 0.5*width for i in x], p99s, width, label='P99', color='darkblue')
    ax.bar([i + 1.5*width for i in x], p999s, width, label='P999', color='navy')

    ax.set_xlabel('Version')
    ax.set_ylabel('Latency (ms)')
    ax.set_title('Latency Percentiles Comparison')
    ax.set_xticks(x)
    ax.set_xticklabels(names)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'latency_comparison.png', dpi=150)
    print(f"Saved: {output_dir / 'latency_comparison.png'}")
    plt.close()

    print(f"\nAll plots saved to: {output_dir}/")

def main():
    parser = argparse.ArgumentParser(description='Compare MultiBatch benchmark results')
    parser.add_argument('--results', nargs='+', required=True,
                       help='JSON result files to compare')
    parser.add_argument('--output', default='./comparison_output',
                       help='Output directory for plots (default: ./comparison_output)')
    parser.add_argument('--no-plots', action='store_true',
                       help='Skip generating plots')

    args = parser.parse_args()

    # Load results
    results = load_results(args.results)

    if not results:
        print("Error: No valid results found")
        return 1

    # Print comparison table
    print_comparison_table(results)

    # Generate plots
    if not args.no_plots:
        try:
            plot_comparison(results, args.output)
        except Exception as e:
            print(f"Warning: Could not generate plots: {e}")
            print("(Install matplotlib: pip install matplotlib)")

    return 0

if __name__ == '__main__':
    sys.exit(main())
