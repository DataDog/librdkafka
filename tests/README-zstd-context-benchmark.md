# Zstd context reuse microbenchmark

`zstd_context_benchmark` measures the exact code path changed by broker-owned
zstd context reuse. It calls `rd_kafka_zstd_compress()` repeatedly with the
current thread marked as the broker I/O thread, while excluding producer
queueing, socket I/O, delivery callbacks, and mock-cluster bookkeeping.

The benchmark reports:

- elapsed time, nanoseconds per compression, and input MiB/s;
- compressed size and a checksum, to catch output changes;
- exact calls to `ZSTD_createCStream()` and `ZSTD_freeCStream()` during the
  measured interval, using linker wrapping rather than allocator-specific
  tooling.

## Build

Use a release build. Development builds intentionally disable optimizations and
may enable sanitizers.

```sh
./configure
make -j"$(nproc)" -C src
make -C tests zstd_context_benchmark
```

The target requires zstd and GNU ld's `--wrap` support. It is intended for the
Linux benchmark host and is not part of the normal test build.

## Compare baseline and candidate

The branch contains the benchmark commit immediately before the context-reuse
commit, so two worktrees provide source-identical benchmark runs against both
implementations:

```sh
git worktree add ../librdkafka-zstd-baseline HEAD^
git worktree add ../librdkafka-zstd-candidate HEAD
```

Configure and build both worktrees with the commands above. Run one benchmark
at a time, pin it to the same otherwise-idle physical CPU, and alternate the
order of baseline and candidate runs to reduce temperature and frequency bias.

```sh
taskset -c 2 ./tests/zstd_context_benchmark \
  --size 262144 \
  --segment-size 4096 \
  --iterations 10000 \
  --warmup 1000 \
  --level 3 \
  --pattern records
```

For the primary case, the baseline should report one context create and free
per measured iteration. The candidate should report zero creates and frees
after warmup. The checksum and compression ratio must match.

For CPU counters, run the same command under `perf stat`:

```sh
perf stat -r 10 \
  -e task-clock,cycles,instructions,cache-references,cache-misses,page-faults \
  taskset -c 2 ./tests/zstd_context_benchmark \
    --size 262144 --segment-size 4096 --iterations 10000 \
    --warmup 1000 --level 3 --pattern records
```

## Suggested matrix

Run at least these cases:

| Input | Pattern | Level | What it emphasizes |
| ---: | --- | ---: | --- |
| 4 KiB | records | 3 | Context lifecycle overhead relative to tiny batches |
| 64 KiB | records | 3 | Common small-to-medium batch behavior |
| 256 KiB | records | 3 | Primary Kafka-like batch case |
| 1 MiB | records | 3 | Compression work dominating context setup |
| 256 KiB | random | 3 | Incompressible input and output-buffer pressure |
| 256 KiB | records | 9 | Higher compression effort |

Collect at least ten samples per case. Compare the median nanoseconds per
compression and cycles; do not use the best single run. A useful result should
show the expected context lifecycle counts, identical output, and no material
regression in any case. The relative CPU improvement should be largest for
small batches, where context setup is a greater fraction of total work.
