# MultiBatch Performance Benchmark

A comprehensive benchmarking framework for measuring and comparing librdkafka producer performance across different implementations.

## Overview

This benchmark measures:
- **Throughput** - Messages/sec and MB/sec
- **ProduceRequest efficiency** - How many requests are sent
- **Batching efficiency** - Messages per ProduceRequest
- **Latency distribution** - P50, P95, P99, P999
- **Per-message latency** - Full distribution for analysis

## Components

### 1. Benchmark Test (`tests/0200-multibatch_benchmark.c`)

C test that:
- Runs warmup phase to establish connections
- Executes multiple measurement runs for statistical significance
- Tracks per-message latency via delivery callbacks
- Captures ProduceRequest count from librdkafka stats
- Outputs JSON results for analysis

**Key features:**
- Configurable partition count (default: 1000)
- Configurable message count per run (default: 100k)
- Multiple runs for confidence intervals
- Support for different distribution patterns (uniform/skewed)

### 2. Comparison Runner (`benchmarks/run_comparison.sh`)

Shell script that:
- Checks out different git commits
- Builds each version
- Runs the benchmark
- Saves results with descriptive names
- Extracts JSON for analysis

### 3. Results Analyzer (`benchmarks/compare_results.py`)

Python script that:
- Loads JSON results from multiple runs
- Generates comparison tables
- Creates visualization plots
- Calculates improvement metrics

## Quick Start

### Running a Single Benchmark

```bash
# Build and run the benchmark test
cd tests
make 0200-multibatch_benchmark
./0200-multibatch_benchmark
```

### Comparing Multiple Commits

```bash
# Compare baseline vs multibatch implementation
cd benchmarks
chmod +x run_comparison.sh

./run_comparison.sh \
    --commits "baseline-commit,multibatch-commit" \
    --names "baseline,multibatch" \
    --output ./results

# Generate comparison plots
python3 compare_results.py \
    --results ./results/*.json \
    --output ./plots
```

### Example: Comparing Three Versions

```bash
# Compare: baseline, multibatch, multibatch+multiple-messagesets
./run_comparison.sh \
    --commits "abc123,def456,ghi789" \
    --names "baseline,multibatch,multi-msgsets" \
    --output ./results_3way

python3 compare_results.py \
    --results ./results_3way/*.json \
    --output ./plots_3way
```

## Configuration

### Benchmark Parameters

Edit `tests/0200-multibatch_benchmark.c`:

```c
bench_config_t config = {
    .partition_cnt = 1000,      // Number of partitions
    .message_cnt = 100000,      // Messages per run
    .message_size = 1024,       // Message payload size
    .num_runs = 5,             // Measurement runs
    .warmup_messages = 10000,   // Warmup phase size
    .distribution = "uniform"   // or "skewed"
};
```

### Quick Mode

For faster iteration during development:

```bash
# Run with reduced message counts
TEST_QUICK=1 ./0200-multibatch_benchmark

# Or with the runner script
./run_comparison.sh --quick --commits "HEAD"
```

## Output Format

### Console Output

```
==========================================
Benchmark Results (averaged over 5 runs)
==========================================
Throughput: 45232.1 msg/sec (44.17 MB/sec)
Requests: 152.4 req/run (656.1 msgs/req)
Latency:
  P50:  12.34 ms
  P95:  45.67 ms
  P99:  89.12 ms
  P999: 152.34 ms
==========================================
```

### JSON Output

Results are output as JSON using `TEST_REPORT()`:

```json
{
  "config": {
    "partition_cnt": 1000,
    "message_cnt": 100000,
    "message_size": 1024,
    "num_runs": 5,
    "distribution": "uniform"
  },
  "results": {
    "throughput_msg_sec": 45232.1,
    "throughput_mb_sec": 44.17,
    "request_count_mean": 152.4,
    "latency_p50_ms": 12.34,
    "latency_p95_ms": 45.67,
    "latency_p99_ms": 89.12,
    "latency_p999_ms": 152.34,
    "messages_per_request": 656.1
  }
}
```

### Comparison Output

The comparison script generates:

**Console table:**
```
==================================================================================================
BENCHMARK COMPARISON
==================================================================================================
Version              Throughput           Requests        Msgs/Req     P50      P99
--------------------------------------------------------------------------------------------------
baseline               23451 msg/sec        9876.0          10.0      45.23    156.78
multibatch             45232 msg/sec         152.4         656.1      12.34     89.12
==================================================================================================

IMPROVEMENTS (vs baseline)
--------------------------------------------------------------------------------------------------
multibatch           Throughput:  1.93x   Request reduction:  64.8x   P99 latency:  1.76x
==================================================================================================
```

**Plots:**
- `throughput_comparison.png` - Throughput bars
- `request_count_comparison.png` - Request count (lower is better)
- `batching_efficiency.png` - Messages per request (higher is better)
- `latency_comparison.png` - Latency percentiles

## Use Cases

### 1. Validate MultiBatch Improvement

```bash
# Compare before and after multibatch implementation
./run_comparison.sh \
    --commits "before-multibatch,after-multibatch" \
    --names "single-partition,multibatch"
```

**Expected results:**
- Request count: 50-100x reduction
- Throughput: 2-5x improvement
- Latency: Similar or better P99

### 2. Validate Multiple MessageSets per Partition

```bash
# Compare multibatch vs multibatch+multiple-msgsets
./run_comparison.sh \
    --commits "multibatch,multi-msgsets" \
    --names "multibatch,multi-msgsets"
```

**Expected results:**
- Request count: Additional 2-10x reduction (depending on queue depth)
- Throughput: 10-30% improvement
- Latency: P99 should not regress

### 3. Test Different Configurations

Edit the benchmark code to test:
- Different `batch.size` values
- Different `linger.ms` values
- Different partition distributions (uniform vs skewed)
- Different message sizes

### 4. Regression Testing

Run benchmark on every major commit:

```bash
# Add to CI/CD
./run_comparison.sh --commits "HEAD" --output "results/$(git rev-parse --short HEAD)"
```

## Interpreting Results

### Key Metrics

**ProduceRequest count** - The smoking gun for batching efficiency
- Lower = better (fewer round trips)
- MultiBatch should reduce this dramatically (50-100x)
- Multiple MessageSets should reduce further

**Messages per Request** - Direct measure of batching
- Higher = better
- Single partition: ~10-100 msgs/req (depends on batch.size)
- MultiBatch: ~500-1000 msgs/req (1000 partitions × 1 msg each)
- Multi-MessageSets: ~5000-10000 msgs/req (if queues are deep)

**Throughput** - Overall speed
- Higher = better
- Should correlate with request reduction
- Diminishing returns after a certain point

**Latency P99** - Tail latency
- Lower = better
- Should not significantly regress with optimizations
- If P99 gets worse, investigate why

### What to Watch For

**Red flags:**
- P99 latency increases significantly (>2x)
- Throughput doesn't improve despite request reduction
- High variance across runs (instability)

**Expected improvements:**
- MultiBatch: 50-100x request reduction, 2-5x throughput
- Multi-MessageSets: Additional 2-10x request reduction

## Advanced Usage

### Custom Workloads

Modify the benchmark to test specific scenarios:

```c
// Hot partition scenario
config.distribution = "skewed";  // 80% to one partition

// Large messages
config.message_size = 1024 * 1024;  // 1MB messages

// Fewer partitions
config.partition_cnt = 10;
```

### Profiling

Run with profiling to identify bottlenecks:

```bash
# CPU profiling
perf record -g ./0200-multibatch_benchmark
perf report

# Memory profiling
valgrind --tool=massif ./0200-multibatch_benchmark
```

### Integration with DataDog

To send metrics to DataDog during benchmarking:

```bash
# Set DogStatsD environment
export DD_AGENT_HOST=localhost
export DD_DOGSTATSD_PORT=8125

# Modify benchmark to send metrics
# (requires code changes to add statsd calls)
```

## Troubleshooting

### Benchmark Won't Build

```bash
# Make sure you're in the test directory
cd tests

# Clean and rebuild
make clean
make 0200-multibatch_benchmark
```

### Broker Connection Issues

The benchmark requires a running Kafka broker. Either:

1. Use trivup (test framework's built-in broker)
2. Point to existing broker via test config
3. Use mock broker (modify test to use mock)

### Inconsistent Results

- Ensure CPU governor is set to performance mode
- Run more iterations (`config.num_runs = 10`)
- Close other applications
- Check for thermal throttling

### Python Script Errors

```bash
# Install dependencies
pip3 install matplotlib

# If matplotlib fails, run without plots
python3 compare_results.py --no-plots --results results/*.json
```

## Contributing

To add new metrics:

1. Modify `bench_ctx_t` to track the metric
2. Update delivery callback or stats callback to collect it
3. Add to `output_results()` JSON output
4. Update `compare_results.py` to visualize it

## License

Same as librdkafka (2-clause BSD)
