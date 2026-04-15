# Kafka Batch Extract

Extract one training sample per Kafka `RecordBatch` from `.log` segment files.

The tool uses Kafka's own record classes from the local Kafka distro, rather
than reimplementing segment parsing in C with all the usual opportunities for
self-harm.

## Output Format

The extractor writes a batch-sample corpus file:

```text
magic[8] = "RDKBAT01"
u32 sample_count (little-endian)
repeat sample_count times:
    u32 sample_len (little-endian)
    sample bytes
```

Each sample is the reconstructed uncompressed `records` payload of a v2 batch,
which matches the byte region librdkafka hands to outer batch compression.

This corpus can be fed directly to
[`payload_dict_train`](../payload_dict/payload_dict_train.c), which now accepts
both the original message corpus format and this batch-sample format.

## Build

Set `KAFKA_HOME` to the root of a Kafka distribution, for example the
`kafka_2.13-3.4.1` directory used by the local cluster.

```bash
make -C tests/tools/kafka_batch_extract \
  KAFKA_HOME=/path/to/kafka_2.13-3.4.1
```

## Run

```bash
make -C tests/tools/kafka_batch_extract \
  KAFKA_HOME=/path/to/kafka_2.13-3.4.1 \
  run \
  ARGS="--max-samples 2500 /tmp/batches.rdkbatch \
        /tmp/kafka-logs-0/topic-0/00000000000000000000.log \
        /tmp/kafka-logs-1/topic-0/00000000000000524288.log"
```

Options:

- `--max-samples N`: stop after `N` extracted batches
- `--include-control-batches`: include control batches instead of skipping them

## Train

```bash
tests/tools/payload_dict/payload_dict_train \
  --sizes 32768,65536,102400 \
  /tmp/batches.rdkbatch \
  /tmp/batch-dicts
```
