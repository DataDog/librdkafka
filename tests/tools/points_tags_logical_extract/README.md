# `points_tags_logical_extract`

Rewrite a `points-tags` record corpus into the logical-message corpus used for
the "replace the app zstd with one dictionary-compressed payload" experiment.

Input:
- existing message corpus format
- each value is the wrapped points-tags payload:
  - `magic`
  - encoded header
  - zstd-compressed binary section
  - zstd-compressed strings section

Output:
- same message corpus format
- same key
- value becomes:
  - `magic + encoded header + raw binary section + raw strings section`

Usage:

```bash
make -C tests/tools/points_tags_logical_extract

tests/tools/points_tags_logical_extract/points_tags_logical_extract \
  --max-messages 50000 \
  /tmp/points-tags-logical.bin \
  /tmp/points-tags-records.bin
```

Typical flow:

```bash
# 1. Extract wrapped record values from Kafka segments.
KAFKA_HOME=/path/to/kafka_2.13-3.4.1 \
make -C tests/tools/kafka_record_extract run \
  ARGS="--max-records 50000 /tmp/points-tags-records.bin \
        /path/to/points-tags-single-mind_normal_2562-6_0 \
        /path/to/points-tags-single-mind_normal_2562-6_1 \
        /path/to/points-tags-single-mind_normal_2562-6_2"

# 2. Rewrite them into logical-message values.
tests/tools/points_tags_logical_extract/points_tags_logical_extract \
  /tmp/points-tags-logical.bin \
  /tmp/points-tags-records.bin

# 3. Train dicts on the logical corpus.
tests/tools/payload_dict/payload_dict_train \
  --sizes 16384,32768,65536,102400 \
  /tmp/points-tags-logical.bin \
  /tmp/points-tags-logical-dicts
```
