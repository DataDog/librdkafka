# Payload Dictionary Trainer

Trains and evaluates zstd dictionaries against:

- the binary message corpus format used by
  `tests/0208-payload_dict_mock.c`
- the batch-sample corpus format emitted by
  [`../kafka_batch_extract`](../kafka_batch_extract/README.md)

Build:

```bash
make -C tests/tools/payload_dict
```

Usage:

```bash
tests/tools/payload_dict/payload_dict_train [options] CORPUS OUTPUT_DIR
```

Examples:

```bash
tests/tools/payload_dict/payload_dict_train \
  --sizes 16384,32768,65536,102400 \
  --train-percent 80 \
  --level 3 \
  tests/kafka-sp-metrics-050a-messages_large_10k_2026_06_10.bin \
  /tmp/payload-dicts
```

Notes:

- For the original message corpus format, training uses message values only.
- For the batch-sample corpus format, each sample is treated as an already
  extracted training unit.
- Evaluation uses a holdout split and compares no-dictionary zstd against each
  trained dictionary at the same compression level.
- Each successful candidate dictionary is written to `OUTPUT_DIR`.
