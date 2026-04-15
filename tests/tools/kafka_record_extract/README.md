# Kafka Record Extract

Extract logical Kafka records from segment files into the message corpus format
used by `tests/0208-payload_dict_mock.c`.

Output format:

- `u32 record_count` (little-endian)
- repeated `record_count` times:
  - `u32 key_len`
  - `u32 value_len`
  - `key bytes`
  - `value bytes`

Build:

```bash
KAFKA_HOME=/path/to/kafka_2.13-3.4.1 make
```

Run:

```bash
KAFKA_HOME=/path/to/kafka_2.13-3.4.1 make run \
  ARGS="--max-records 50000 /tmp/records.bin /path/to/segment0 /path/to/segment1"
```

The extractor skips control batches by default and only emits records from v2
batches. That keeps it aligned with the modern batch format we care about.
