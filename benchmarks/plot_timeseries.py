#!/usr/bin/env python3
"""
Plot Latency Time Series from Benchmark

Generates scatter plots showing latency over time during the benchmark run.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import sys
import glob
from pathlib import Path

def plot_timeseries(csv_files, output_file='latency_timeseries.png'):
    """Generate time series scatter plots from CSV files"""

    # Read all CSV files (skip run1 which is warmup)
    dfs = []
    for csv_file in csv_files:
        # Skip run1 (warmup run)
        if 'run1.csv' in csv_file:
            print(f"Skipping warmup run: {csv_file}")
            continue

        df = pd.read_csv(csv_file)
        df['run'] = Path(csv_file).stem.split('_')[-1]  # Extract run number
        dfs.append(df)

    if not dfs:
        print("No CSV files found (after skipping warmup)")
        return

    # Combine all runs
    all_data = pd.concat(dfs, ignore_index=True)

    # Convert latency from microseconds to milliseconds
    all_data['latency_ms'] = all_data['latency_us'] / 1000.0

    # Create figure with subplots
    num_runs = len(dfs)
    fig, axes = plt.subplots(num_runs, 1, figsize=(14, 4 * num_runs), squeeze=False)
    fig.suptitle('Latency Over Time (per run, warmup excluded)', fontsize=16, fontweight='bold')

    for idx, ax in enumerate(axes.flat):
        df = dfs[idx]
        df['latency_ms'] = df['latency_us'] / 1000.0
        run_num = df['run'].iloc[0] if 'run' in df else idx + 2  # +2 because we skip run 1

        # Scatter plot
        ax.scatter(df['relative_time_ms'], df['latency_ms'],
                  alpha=0.4, s=1, c='steelblue', rasterized=True)

        # Calculate rolling median for trend line
        if len(df) > 100:
            window = min(1000, len(df) // 10)
            df_sorted = df.sort_values('relative_time_ms')
            df_sorted['latency_median'] = df_sorted['latency_ms'].rolling(window=window, center=True).median()
            ax.plot(df_sorted['relative_time_ms'], df_sorted['latency_median'],
                   'r-', linewidth=2, alpha=0.7, label=f'Rolling median (window={window})')

        ax.set_xlabel('Time (ms)')
        ax.set_ylabel('Latency (ms)')
        ax.set_title(f'Run {run_num} - {len(df)} messages')
        ax.grid(True, alpha=0.3)
        ax.legend()

        # Add statistics text
        stats_text = f'P50: {df["latency_ms"].quantile(0.5):.2f}ms\n'
        stats_text += f'P95: {df["latency_ms"].quantile(0.95):.2f}ms\n'
        stats_text += f'P99: {df["latency_ms"].quantile(0.99):.2f}ms'
        ax.text(0.02, 0.98, stats_text,
               transform=ax.transAxes,
               verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
               fontsize=9)

    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    print(f"Saved: {output_file}")
    plt.close()


def plot_combined_timeseries(csv_files, output_file='latency_timeseries_combined.png'):
    """Generate combined scatter plot with all runs overlaid"""

    # Filter out warmup run (run1)
    filtered_files = [f for f in csv_files if 'run1.csv' not in f]

    if not filtered_files:
        print("No CSV files found (after skipping warmup)")
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle('Combined Latency Time Series (runs 2-6, warmup excluded)', fontsize=16, fontweight='bold')

    colors = plt.cm.tab10(range(len(filtered_files)))

    # Plot 1: All messages
    for idx, csv_file in enumerate(filtered_files):
        df = pd.read_csv(csv_file)
        df['latency_ms'] = df['latency_us'] / 1000.0
        run_num = Path(csv_file).stem.split('_')[-1].replace('run', '')

        ax1.scatter(df['msg_seq'], df['latency_ms'],
                   alpha=0.3, s=1, c=[colors[idx]],
                   label=f'Run {run_num}', rasterized=True)

    ax1.set_xlabel('Message Sequence Number')
    ax1.set_ylabel('Latency (ms)')
    ax1.set_title('Latency by Message Sequence')
    ax1.grid(True, alpha=0.3)
    ax1.legend(markerscale=10)

    # Plot 2: Histogram of latencies
    for idx, csv_file in enumerate(filtered_files):
        df = pd.read_csv(csv_file)
        df['latency_ms'] = df['latency_us'] / 1000.0
        run_num = Path(csv_file).stem.split('_')[-1].replace('run', '')

        ax2.hist(df['latency_ms'], bins=100, alpha=0.5,
                label=f'Run {run_num}', color=colors[idx])

    ax2.set_xlabel('Latency (ms)')
    ax2.set_ylabel('Frequency')
    ax2.set_title('Latency Distribution')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.legend()

    plt.tight_layout()
    plt.savefig(output_file, dpi=150)
    print(f"Saved: {output_file}")
    plt.close()


def main():
    if len(sys.argv) > 1:
        # CSV files specified on command line
        csv_files = sys.argv[1:]
    else:
        # Find all latency CSV files in current directory and tests directory
        csv_files = glob.glob('latency_timeseries_run*.csv')
        if not csv_files:
            csv_files = glob.glob('tests/latency_timeseries_run*.csv')

        if not csv_files:
            print("No latency_timeseries_run*.csv files found")
            print("Usage: python3 plot_timeseries.py [csv_file1.csv csv_file2.csv ...]")
            return 1

    csv_files = sorted(csv_files)
    print(f"Found {len(csv_files)} CSV file(s):")
    for f in csv_files:
        print(f"  - {f}")

    # Generate plots
    plot_timeseries(csv_files, 'latency_timeseries.png')
    plot_combined_timeseries(csv_files, 'latency_timeseries_combined.png')

    print("\nPlots generated:")
    print("  - latency_timeseries.png (per-run plots)")
    print("  - latency_timeseries_combined.png (combined view)")

    return 0


if __name__ == '__main__':
    sys.exit(main())
