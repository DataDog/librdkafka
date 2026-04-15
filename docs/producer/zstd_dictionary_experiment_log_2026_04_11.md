# Zstd Dictionary Experiment Log, 2026-04-11

## Goal

Evaluate whether zstd dictionaries can reduce producer bytes for:

- applications using only Kafka outer compression, `compression.codec=zstd`
- applications already doing app-side compression inside custom payload formats

The working benchmark harness is the manual test [0208-payload_dict_mock.c](/home/bits/go/src/github.com/DataDog/librdkafka/tests/0208-payload_dict_mock.c).


## Short Conclusion

- Generic payload dictionaries are most promising for producers that are **not already doing app-side compression**.
- For producers already doing custom app-side compression, generic dictionary gains were small.
- For `index-routed` tagset V9 payloads, the best result on exact serialized message values was about **2.14% lower `tx_bytes`** and **2.72% lower `txmsg_bytes`**, with fewer requests.
- Outer-batch dictionaries on already app-compressed data were weaker, around **1-2%**.
- Do **not** use `kafka-console-consumer` to build binary corpora unless you like corrupted bytes and false conclusions.


## Important Corpus Caveat

The file [tests/kafka-sp-metrics-050a-messages_larger_10k_2026_06_10.bin](/home/bits/go/src/github.com/DataDog/librdkafka/tests/kafka-sp-metrics-050a-messages_larger_10k_2026_06_10.bin) is not a faithful binary dump of `index-routed` payloads.

Observed problems:

- the first record starts with `Picked up JAVA_TOOL_OPTI`
- many values start with `ef bf bd`, UTF-8 replacement characters
- only `485 / 50000` messages in that file look like tagset V9 payloads
- in a real `index-routed` segment extract, `50000 / 50000` messages look like V9 and all start with `09 02`

Conclusion:

- use binary-safe segment extraction for real experiments
- do not trust console-consumer output for binary payload benchmarking


## What We Built

### Benchmark Harness

- [0208-payload_dict_mock.c](/home/bits/go/src/github.com/DataDog/librdkafka/tests/0208-payload_dict_mock.c)

Supports:

- baseline outer Kafka `zstd`
- payload dictionary compression plus outer `none`
- outer Kafka `zstd` plus outer-batch dictionary
- mock cluster and real cluster modes

Useful env vars:

- `CORPUS_PATH`
- `DICT_PATH`
- `DICT_LEVEL`
- `DICT_BENCH_MODE`
- `PAYLOAD_DICT_OUTER_COMPRESSION`
- `OUTER_COMPRESSION_LEVEL`
- `PAYLOAD_DICT_USE_REAL_CLUSTER`


### Message Corpus Trainer

- [payload_dict_train.c](/home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/payload_dict/payload_dict_train.c)

Inputs:

- message corpus:
  - `u32 message_count`
  - repeated `u32 key_len`, `u32 value_len`, key bytes, value bytes
- batch corpus:
  - magic `RDKBAT01`
  - `u32 sample_count`
  - repeated `u32 sample_len`, sample bytes


### Kafka Segment Extractors

- batch extractor:
  - [RecordBatchExtract.java](/home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_batch_extract/RecordBatchExtract.java)
- record extractor:
  - [RecordExtract.java](/home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_record_extract/RecordExtract.java)

Batch extractor emits one sample per Kafka `RecordBatch`, reconstructed as the uncompressed `records` payload.

Record extractor emits the original message corpus format used by `0208`.


## Commands

### Build the Extractors

```bash
KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_batch_extract

KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_record_extract
```


### Extract Batch Samples from Kafka Log Segments

Example, `800` batches per segment:

```bash
KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_batch_extract run \
ARGS="--max-samples 800 /tmp/index-routed-batches-0.rdkbatch /home/bits/dd/librdkafka/index-routed-single-mind_normal_97f3-6_0"

KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_batch_extract run \
ARGS="--max-samples 800 /tmp/index-routed-batches-1.rdkbatch /home/bits/dd/librdkafka/index-routed-single-mind_normal_97f3-6_1"

KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_batch_extract run \
ARGS="--max-samples 800 /tmp/index-routed-batches-2.rdkbatch /home/bits/dd/librdkafka/index-routed-single-mind_normal_97f3-6_2"
```

If `/tmp` is tight, sample per file and merge later instead of extracting the whole planet in one shot.


### Train Dicts on a Batch Corpus

```bash
/home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/payload_dict/payload_dict_train \
  --sizes 32768,65536,102400 \
  /tmp/index-routed-batches-merged.rdkbatch \
  /tmp/index-routed-batch-dicts
```


### Extract a Record Corpus from Kafka Log Segments

```bash
KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/kafka_record_extract run \
ARGS="--max-records 50000 /tmp/index-routed-records-bench.bin /home/bits/dd/librdkafka/index-routed-single-mind_normal_97f3-6_2"
```


### Train Dicts on Exact Serialized Message Values

```bash
/home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/payload_dict/payload_dict_train \
  --sizes 16384,32768,65536,102400 \
  /tmp/index-routed-records-bench.bin \
  /tmp/index-routed-value-dicts
```


### Run the Manual Benchmark, Payload Dict Candidate

```bash
CORPUS_PATH=/tmp/index-routed-records-bench.bin \
DICT_PATH=/tmp/index-routed-value-dicts/dict-100k.zstd \
DICT_LEVEL=3 \
PAYLOAD_DICT_OUTER_COMPRESSION=none \
TESTS=0208 make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests run_seq
```


### Run the Manual Benchmark, Outer-Batch Dict Candidate

```bash
CORPUS_PATH=/tmp/index-routed-records-bench.bin \
DICT_PATH=/tmp/index-routed-batch-dicts/dict-100k.zstd \
DICT_BENCH_MODE=outer-zstd \
OUTER_COMPRESSION_LEVEL=3 \
TESTS=0208 make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests run_seq
```


## Storage-Router Findings

The `index-routed` topic is written by `storage-router`.

Relevant code:

- [main.go](/home/bits/go/src/github.com/DataDog/dd-go/apps/merle/cmd/storage-router/main.go#L169)
- [cmd.go](/home/bits/go/src/github.com/DataDog/dd-go/apps/merle/cmd/cmd.go#L181)
- [tagset.go](/home/bits/go/src/github.com/DataDog/dd-go/apps/merle/internal/app/forwarder/format/tagset.go#L142)
- [v9.go](/home/bits/go/src/github.com/DataDog/dd-go/db/kafka/wireformat/tagset/v9.go#L129)
- [compression_utils.go](/home/bits/go/src/github.com/DataDog/dd-go/db/kafka/wireformat/tagset/compression_utils.go#L26)

Key points:

- `dest_compression_codec=none` only disables Kafka outer batch compression
- the `index` path uses `TagsetsPldType`
- the serializer builds a `tagset.PayloadV9`
- `tagset.PayloadV9` compresses its body with app-side zstd
- the first two bytes `09 02` mean:
  - `09` = payload version V9
  - `02` = `CompressionZstd`

So these payloads are already app-compressed before Kafka sees them.


## Results

### 1. Earlier `points-tags` / metrics-style topic

Using a value-trained dict on [tests/kafka-sp-metrics-050a-messages_larger_10k_2026_06_10.bin](/home/bits/go/src/github.com/DataDog/librdkafka/tests/kafka-sp-metrics-050a-messages_larger_10k_2026_06_10.bin):

- payload bytes got much smaller
- total wire bytes got worse
- outer Kafka `zstd` still won

One representative result:

- baseline outer `zstd`: `tx_bytes=5,866,510`, `txmsg_bytes=11,427,326`
- payload dict + outer `lz4`: `tx_bytes=7,186,246`, `txmsg_bytes=7,794,287`

This was part of the clue that payload-level dict compression can destroy outer batch compression leverage.


### 2. `points-tags-single-mind_normal_2562-6_*`, outer-batch dict

Training on `RecordBatch` samples from the real `points-tags` segments produced only tiny wins.

Representative results:

- batch-corpus holdout improvement vs plain outer zstd: about `1.43%`
- actual `0208` producer-side `tx_bytes` improvement:
  - held-out segment replay: about `0.10%`
  - same-segment replay: about `0.13%`

Conclusion:

- outer-batch dicts on already app-compressed `NKJ3/NKJ4` payloads are basically noise


### 3. `index-routed-single-mind_normal_97f3-6_*`, outer-batch dict

Training on `2400` `RecordBatch` samples, `800` from each segment:

- `32k`: `1.49%`
- `64k`: `1.86%`
- `100k`: `1.98%`

Best dict:

- `/tmp/index-routed-batch-dicts/dict-100k.zstd`

Benchmark result in `0208`, outer-batch dict mode:

- baseline outer `zstd`: `txmsg_bytes=495,239,557`, `tx_bytes=492,954,530`, `produce_reqs=499`
- outer `zstd` + batch-trained dict: `txmsg_bytes=495,239,557`, `tx_bytes=486,465,683`, `produce_reqs=499`

Improvement:

- `candidate/baseline tx_bytes = 0.9868`
- about `1.32%` better on wire bytes


### 4. `index-routed`, payload dict using the wrong dict shape

Using the `RecordBatch`-trained dict as a per-message payload dict still helped a little:

- baseline outer `zstd`: `tx_bytes=492,954,557`
- payload dict + outer `none`, using the batch-trained dict: `tx_bytes=488,398,463`

Improvement:

- `candidate/baseline tx_bytes = 0.9908`
- about `0.92%`

This was scientifically cursed but mildly interesting.


### 5. `index-routed`, payload dict trained on exact serialized values

This was the cleanest result for the topic.

Training on the exact serialized values from `/tmp/index-routed-records-bench.bin`:

- `16k`: `1.14%`
- `32k`: `1.64%`
- `64k`: `2.29%`
- `100k`: `2.78%`

Best dict:

- `/tmp/index-routed-value-dicts/dict-100k.zstd`

Benchmark result in `0208`, payload dict mode:

- baseline outer `zstd`: `txmsg_bytes=495,239,557`, `tx_bytes=492,954,553`, `produce_reqs=499`
- payload dict + outer `none`: `txmsg_bytes=481,793,315`, `tx_bytes=482,413,012`, `produce_reqs=485`

Improvement:

- `candidate/baseline tx_bytes = 0.9786`
- `candidate/baseline txmsg_bytes = 0.9728`
- wire bytes improved about `2.14%`
- payload bytes improved about `2.72%`
- requests dropped `499 -> 485`


### 6. Cross-corpus sanity check

Using the `index-routed` value-trained dict on the older corrupted metrics-style corpus:

- baseline outer `zstd`: `txmsg_bytes=11,427,326`, `tx_bytes=5,864,127`
- candidate payload dict + outer `none`: `txmsg_bytes=10,066,501`, `tx_bytes=10,563,326`

Result:

- payload bytes improved
- total wire bytes got about `80%` worse

This was good news. It means the `index-routed` dict is not some magical benchmark artifact that wins everywhere.


### 7. `points-tags`, replacing the app zstd layout with one logical-message dict

For `points-tags`, the stored Kafka value is not a raw zstd blob. It is the
`NKJ3`/`NKJ4` metrics payload format:

- `magic`
- encoded header
- zstd-compressed binary section
- zstd-compressed strings section

The old outer-batch experiments on these values were valid for "exact on-wire"
questions, but they were the wrong experiment for "beat the current app zstd".

We added:

- `tests/tools/points_tags_logical_extract/points_tags_logical_extract.c`

This rewrites an extracted record corpus into a new message corpus whose value
is:

- `magic + encoded header + raw binary section + raw strings section`

That is intentionally not the original wire format. It is the benchmark corpus
for "replace the current inner zstd layout with one dict-compressed payload".

Commands used:

```bash
# Extract wrapped values from Kafka log segments.
KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C tests/tools/kafka_record_extract run \
  ARGS="--max-records 100000 /tmp/points-tags-train-records.bin \
        /home/bits/dd/librdkafka/points-tags-single-mind_normal_2562-6_0 \
        /home/bits/dd/librdkafka/points-tags-single-mind_normal_2562-6_1"

KAFKA_HOME=/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/kafka_pkg/kafka_2.13-3.4.1 \
make -C tests/tools/kafka_record_extract run \
  ARGS="--max-records 10000 /tmp/points-tags-bench-records-10k.bin \
        /home/bits/dd/librdkafka/points-tags-single-mind_normal_2562-6_2"

# Rewrite wrapped values into logical messages.
tests/tools/points_tags_logical_extract/points_tags_logical_extract \
  --max-messages 20000 \
  /tmp/points-tags-train-logical-20k.bin \
  /tmp/points-tags-train-records.bin

tests/tools/points_tags_logical_extract/points_tags_logical_extract \
  /tmp/points-tags-bench-logical-10k.bin \
  /tmp/points-tags-bench-records-10k.bin

# Train dicts on the logical messages.
tests/tools/payload_dict/payload_dict_train \
  --sizes 16384,32768,65536,102400 \
  /tmp/points-tags-train-logical-20k.bin \
  /tmp/points-tags-logical-dicts

# Benchmark current wrapped values vs logical-message dict compression.
BASELINE_CORPUS_PATH=/tmp/points-tags-bench-records-10k.bin \
CANDIDATE_CORPUS_PATH=/tmp/points-tags-bench-logical-10k.bin \
DICT_PATH=/tmp/points-tags-logical-dicts/dict-100k.zstd \
BASELINE_COMPRESSION_TYPE=none \
DICT_LEVEL=3 \
PAYLOAD_DICT_OUTER_COMPRESSION=none \
TESTS=0208 make -C tests run_seq
```

Training result on the 20k logical-message train corpus:

- `16k`: `10.72%`
- `32k`: `12.40%`
- `64k`: `15.44%`
- `100k`: `17.35%`

Best dict:

- `/tmp/points-tags-logical-dicts/dict-100k.zstd`

Benchmark result on the held-out 10k wrapped-vs-logical corpora:

- baseline current wrapped payloads, outer Kafka compression `none`:
  - `txmsg_bytes=44,658,791`
  - `tx_bytes=44,763,299`
  - `produce_reqs=18`
- candidate logical-message dict compression, outer Kafka compression `none`:
  - `txmsg_bytes=40,422,435`
  - `tx_bytes=40,527,672`
  - `produce_reqs=16`

Improvement:

- `candidate/baseline tx_bytes = 0.9054`
- `candidate/baseline txmsg_bytes = 0.9051`
- about `9.5%` better on wire bytes
- request count dropped `18 -> 16`

This is the first result that looks materially interesting for a payload that
was already doing app-owned zstd. The catch is obvious: this is not a generic
"turn dicts on" win, it is a format-specific replacement experiment.


## Recommended Direction

### Good Next Targets

Producers that:

- use Kafka outer `compression.codec=zstd`
- do **not** already do app-side compression
- have raw or mostly raw payloads

These are the best candidates for a generic dictionary experiment.


### Bad Generic Targets

Producers already doing app-side compression inside custom formats:

- tagset V9
- `NKJ3` / `NKJ4`
- similar app-owned wrappers

For these, generic dict training is probably not worth auto-applying.

If they ever use dictionaries, it should likely be:

- serializer-specific
- app-owned
- explicitly rolled out by the team that owns the format

The `points-tags` logical-message experiment above is exactly that kind of
serializer-specific work. It is promising, but it should not be mistaken for a
generic librdkafka-level feature win.

## Full-Segment Replay Results

After the held-out 10k benchmark, I ran the full real-cluster replay against
the three production `points-tags-single-mind_normal_2562-6_*` segment files.
Baseline was measured directly from the existing `.log` segment bytes rather
than replaying the wrapped corpus.

Baseline segment bytes:

- `/home/bits/dd/librdkafka/points-tags-single-mind_normal_2562-6_0`:
  `536,829,290`
- `/home/bits/dd/librdkafka/points-tags-single-mind_normal_2562-6_1`:
  `536,855,776`
- `/home/bits/dd/librdkafka/points-tags-single-mind_normal_2562-6_2`:
  `536,845,019`
- total: `1,610,530,085`

Candidate 1, logical-message payload dict compression:

- replay test: `0209_points_tags_replay_real`
- mode: `POINTS_TAGS_REPLAY_MODE=payload-dict`
- corpus: full wrapped-record extraction rewritten to logical messages
- dict: `/tmp/points-tags-logical-dicts/dict-100k.zstd`
- candidate topic segment bytes: `1,408,746,289`
- improvement vs baseline: `12.53%`

Candidate 2, logical-message outer Kafka `zstd` plus outer dict:

- replay test: `0209_points_tags_replay_real`
- mode: `POINTS_TAGS_REPLAY_MODE=outer-zstd-dict`
- result: rejected by the stock broker
- broker-side failure:
  - `Failed to decompress record stream`
  - `Decompression error: Dictionary mismatch`

The exact broker error showed up in:

- `/home/bits/.cache/bazel/_bazel_bits/d1ec228f16c3648d710a1b9904b415c9/execroot/dd_source/bazel-out/k8-fastbuild/bin/domains/kafka/kafka-cluster-3_kafka.sh.runfiles/dd_source/logs/server.log`

This confirms the expected layering boundary:

- payload-level dict compression can be benchmarked against an unmodified broker
- outer Kafka `zstd` with a custom dict cannot be shipped against an unmodified
  broker, because the broker tries to decompress the batch during append and
  does not have the dictionary

To estimate whether patching the broker would be worth it at all, I ran the
same full-segment comparison through the mock-cluster benchmark instead:

- baseline corpus: full wrapped `points-tags` messages
- candidate corpus: full logical-message rewrite
- baseline transport: `compression.type=none`
- candidate transport: `compression.type=zstd` plus outer dict

Results from `0208_payload_dict_mock` on the full corpora:

- baseline current wrapped messages:
  - `txmsg_bytes=1,557,940,762`
  - `tx_bytes=1,561,831,145`
  - `produce_reqs=599`
- candidate logical messages with outer Kafka `zstd` plus dict:
  - `txmsg_bytes=7,359,772,149`
  - `tx_bytes=1,304,331,713`
  - `produce_reqs=2,888`

Comparison:

- `candidate/baseline tx_bytes = 0.8351`
- about `16.49%` smaller than the current wrapped baseline on the wire

This is better than the full real-cluster payload-dict replacement result
(`12.53%` smaller on disk), but it is only a producer-side mock-cluster
estimate. It says the broker patch might be worth considering, not that the
unmodified broker can accept it.

## Larger Dictionary Sweep On 100k Logical Samples

The earlier `points-tags` winner was trained on `20,000` logical-message
samples and topped out at `100KB`. I reran training on a much larger logical
corpus:

- corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-train-logical-100k.bin`
- total samples: `100,000`
- total sample bytes: `1,946,363,435`
- default trainer split: `80,000` train / `20,000` holdout
- train bytes: `1,549,408,240`
- holdout raw bytes: `396,955,195`

First sweep:

- `102400`: `18.00%`
- `112640`: `18.44%`
- `131072`: `18.94%`
- `163840`: `19.58%`
- `196608`: `20.03%`
- `262144`: `22.26%`
- `307200`: `22.81%`

Second sweep:

- `307200`: `22.81%`
- `360448`: `23.09%`
- `393216`: `22.97%`
- `458752`: `23.22%`
- `524288`: `23.35%`

Winner:

- `/tmp/points-tags-logical-dicts-100k-samples-512k/dict-512k.zstd`

So for this workload, larger dictionaries kept helping, though the gains
flattened out above roughly `256KB`.

## Reruns With 512KB Dict

### Real-Cluster Payload-Dict Replay

Rerunning the full `points-tags` payload-dict replay with the `512KB` logical
dict:

- replay test: `0209_points_tags_replay_real`
- mode: `POINTS_TAGS_REPLAY_MODE=payload-dict`
- corpus: `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-records.bin`
- dict:
  `/tmp/points-tags-logical-dicts-100k-samples-512k/dict-512k.zstd`
- candidate topic segment bytes: `1,309,243,277`
- improvement vs baseline `1,610,530,085`: `18.71%`

This materially beat the earlier `100KB` result (`12.53%` smaller on disk).

### Mock Outer-Zstd-Dict Comparison

I also reran the full mock-cluster outer Kafka `zstd` plus dict comparison with
the `512KB` logical dict:

- baseline current wrapped messages:
  - `txmsg_bytes=1,557,940,762`
  - `tx_bytes=1,561,830,546`
  - `produce_reqs=599`
- candidate logical messages with outer Kafka `zstd` plus dict:
  - `txmsg_bytes=7,359,772,149`
  - `tx_bytes=1,311,883,113`
  - `produce_reqs=2,888`

Comparison:

- `candidate/baseline tx_bytes = 0.8400`
- about `16.00%` smaller than the current wrapped baseline on the wire

This was actually slightly worse than the earlier `100KB` mock outer result
(`16.49%` smaller). So the larger logical-message dict clearly helped the
payload-dict replacement path, but did not improve the outer Kafka `zstd` dict
path.

## Outer-Zstd Batch-Shaped Training

The logical-message dict was the wrong training distribution for the outer Kafka
`zstd` path. To fix that, I added a hidden sample-dump hook in librdkafka:

- config: `ut.compression.zstd.sample.dump.path`
- output format: existing `RDKBAT01` batch-sample corpus
- sample unit: exact serialized batch bytes immediately before outer `zstd`
  compression

I then replayed the full logical `points-tags` corpus through `0208` with:

- baseline corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-logical.bin`
- `BASELINE_COMPRESSION_TYPE=zstd`
- dump path:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-outer-batches-full.rdkbatch`

That produced:

- `2888` batch samples
- `7,363,450,385` bytes on disk for the dumped corpus

The full corpus was too large for `ZDICT_trainFromBuffer()` as used by the
trainer, so I created an evenly spaced `1024`-batch subset:

- subset corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-outer-batches-sample-1024.rdkbatch`
- `1024` samples
- `2,612,428,897` total sample bytes

Training results on that batch-shaped corpus:

- `32768`: `0.79%`
- `65536`: `1.43%`
- `102400`: `2.00%`
- `131072`: `2.41%`
- `196608`: `3.12%`
- `262144`: `3.54%`
- `393216`: `4.01%`
- `524288`: `1.37%`

Winner:

- `/tmp/points-tags-outer-batch-dicts-1024/dict-384k.zstd`

This is the first outer-dict result trained on the correct compression unit.
Two useful takeaways:

- message-shaped training was the wrong model for outer Kafka `zstd`
- larger is not monotonic here, `512KB` was worse than `384KB`

### Mock Outer Benchmark With Batch-Shaped Dict

I reran the full mock-cluster outer comparison using:

- baseline current wrapped messages:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-records.bin`
- candidate logical messages:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-logical.bin`
- outer dict:
  `/tmp/points-tags-outer-batch-dicts-1024/dict-384k.zstd`
- `BASELINE_COMPRESSION_TYPE=none`
- candidate mode: `DICT_BENCH_MODE=outer-zstd`
- `OUTER_COMPRESSION_LEVEL=3`

Results:

- baseline wrapped messages:
  - `txmsg_bytes=1,557,940,762`
  - `tx_bytes=1,561,831,145`
  - `produce_reqs=599`
- candidate logical messages with outer Kafka `zstd` plus batch-trained dict:
  - `txmsg_bytes=7,359,772,149`
  - `tx_bytes=1,276,222,665`
  - `produce_reqs=2,888`

Comparison:

- `candidate/baseline tx_bytes = 0.8171`
- about `18.29%` smaller than the current wrapped baseline on the wire

This materially beat the earlier outer result that used a message-trained dict.


### Practical Guidance

- capture binary payload corpora with segment extraction, not console-consumer
- for raw-value producers, train on exact serialized values first
- only keep a topic if the win is large enough to justify rollout complexity
- do not generalize from corrupted corpora


## Temporary Files Used During These Experiments

Representative scratch files:

- `/tmp/index-routed-records-bench.bin`
- `/tmp/index-routed-batch-dicts`
- `/tmp/index-routed-value-dicts`
- `/tmp/index-routed-batches-*.rdkbatch`

If `/tmp` fills up, delete only obvious experiment artifacts and stale temp `.so` blobs. Do not go full chainsaw on random files unless you enjoy finding out what they were for later.


## Actual Producer Knobs, Outer-Zstd Batch Training

The previous outer-dict training used a scaled-up batch profile to compensate
for the larger logical messages. That is a reasonable modeling choice for
training, but not the final "what happens if we actually use the original
producer knobs" question.

For that actual-usage proxy, I matched the original producer settings:

- `linger.ms=300`
- `max.in.flight=1`
- `batch.num.messages=10000`
- `batch.size=512000`
- `message.max.bytes=1048472`
- `queue.buffering.max.kbytes=600000`
- `queue.buffering.max.messages=600000`
- `sticky.partitioning.linger.ms=3000`

I then dumped pre-outer-zstd batch samples from a full logical replay under
those exact settings. The full corpus came out to:

- full dump corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-outer-batches-actual-cap-full.rdkbatch`
- `9320` batch samples
- `4,117,065,728` bytes on disk

That was a bit too large for the current trainer path, so I created an evenly
spaced subset just under the practical limit:

- subset corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-outer-batches-actual-cap-8192.rdkbatch`
- `8192` batch samples
- `3,618,320,474` bytes on disk

Training sizes:

- `65536`
- `102400`
- `131072`
- `196608`
- `262144`
- `393216`
- `524288`

Holdout improvement vs plain outer zstd on the batch corpus:

- `65536`: `5.91%`
- `102400`: `7.75%`
- `131072`: `8.21%`
- `196608`: `9.31%`
- `262144`: `12.18%`
- `393216`: `13.07%`
- `524288`: `12.98%`

Winner:

- `/tmp/points-tags-outer-batch-dicts-actual-cap-8192/dict-384k.zstd`

So the actual-cap outer path still wants a fairly large dict, but it tops out
around `384KB` rather than improving forever.


## Actual Producer Knobs, Final Comparison

With the original producer knobs above, the two viable candidate paths were:

- payload-dict replacement:
  logical message corpus, per-message dict compression, outer Kafka
  compression `none`
- outer-zstd-dict:
  logical message corpus, outer Kafka `zstd` plus a batch-trained dict

Both were compared against the current wrapped baseline corpus, with the same
mock-cluster transport stats.

### Payload-Dict Under Actual Producer Knobs

Inputs:

- baseline corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-records.bin`
- candidate logical corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-logical.bin`
- dict:
  `/tmp/points-tags-logical-dicts-100k-samples-512k/dict-512k.zstd`

Results:

- baseline wrapped messages:
  - `tx_bytes=1,562,083,883`
  - `produce_reqs=3127`
- candidate payload-dict:
  - `txmsg_bytes=1,305,256,664`
  - `tx_bytes=1,309,358,061`
  - `produce_reqs=2622`

Comparison:

- `candidate/baseline tx_bytes = 0.8382`
- about `16.18%` fewer wire bytes
- request count also improved, `3127 -> 2622`


### Outer-Zstd-Dict Under Actual Producer Knobs

Inputs:

- baseline corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-records.bin`
- candidate logical corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-logical.bin`
- outer dict:
  `/tmp/points-tags-outer-batch-dicts-actual-cap-8192/dict-384k.zstd`

Results:

- baseline wrapped messages:
  - `tx_bytes=1,562,083,883`
  - `produce_reqs=3127`
- candidate logical messages with outer Kafka `zstd` plus dict:
  - `tx_bytes=1,291,762,248`
  - `produce_reqs=16655`

Comparison:

- `candidate/baseline tx_bytes = 0.8269`
- about `17.31%` fewer wire bytes
- request count got much worse, `3127 -> 16655`


### Practical Read

Under the original producer knobs:

- outer `zstd` plus dict wins on raw wire bytes, but only by about `1.13`
  points over payload-dict
- payload-dict is much simpler to ship, works on a stock broker, and keeps the
  request rate healthier

So the surprisingly durable result is that per-message compression is still the
more practical winner, even though the outer path can squeeze a bit more out of
the wire if you are willing to pay for broker changes and a much uglier request
shape.


### Important Operational Note

The `produce_reqs` blow-up on the outer path is not some weird artifact. It is
the direct result of how librdkafka batches producer records:

- `batch.size`
- `batch.num.messages`
- `message.max.bytes`

are enforced on the uncompressed record bytes, before outer Kafka `zstd`
compression runs.

That means:

- payload-dict helps batching, because the producer sees the smaller
  per-message bytes up front
- outer `zstd` plus dict does not help batching, because the producer still
  has to cut batches using the much larger logical message bytes

So if a team wants to make the outer path viable in practice, they would need
to retune the producer for the larger logical messages, at minimum:

- `batch.size`
- `message.max.bytes`
- possibly `queue.buffering.max.kbytes`
- possibly broker/topic `max.message.bytes` if the request shape changes enough

And if those knobs change, the outer dict should be retrained on batch samples
captured under the new settings. Otherwise the dict is being trained on the
wrong compression unit again, which is how one accidentally does performance art
instead of engineering.


### Request-Matched Outer Retune

There is one more fair lens for the outer path:

- do not reuse the literal `512KB` cap from the current producer
- instead, scale the cap so the outer path lands at about the same
  `produce_reqs` as the current wrapped baseline

Using the observed logical-to-wrapped size ratio of about `4.72x`, I set:

- `batch.size=2418708`
- `message.max.bytes=4953022`

and kept the rest of the original producer profile:

- `linger.ms=300`
- `max.in.flight=1`
- `batch.num.messages=10000`
- `queue.buffering.max.kbytes=600000`
- `queue.buffering.max.messages=600000`
- `sticky.partitioning.linger.ms=3000`

I used the batch-trained outer dict from the larger-batch profile:

- `/tmp/points-tags-outer-batch-dicts-1024/dict-384k.zstd`

Result:

- current wrapped baseline under the original producer knobs:
  - `tx_bytes=1,562,083,883`
  - `produce_reqs=3127`
- outer `zstd` plus dict with the retuned cap:
  - `tx_bytes=1,327,801,550`
  - `produce_reqs=3135`

So:

- request count was effectively matched, `3135` vs `3127`
- wire-byte improvement vs the original wrapped baseline was `15.00%`

That is the useful correction to the earlier outer result:

- the scary `16655` request count was mostly a consequence of forcing the much
  larger logical messages through the old `512KB` pre-compression cap
- if you retune the cap to be fair to the larger pre-compression message shape,
  the request blow-up mostly disappears

But the more important conclusion is this:

- payload-dict under the original producer knobs still did better on bytes,
  `16.18%` smaller
- request-matched outer `zstd` plus dict did `15.00%`

So once the comparison is made fair on request count, payload-dict is not just
the simpler path. It also wins on wire bytes.


### Request-Matched Retraining

I also retrained the outer dict on the exact request-matched batch profile
instead of reusing the earlier larger-batch-trained dict.

Request-matched dump corpus:

- full dump:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-outer-batches-reqmatched-full.rdkbatch`
- `3135` samples
- `7,363,436,409` bytes on disk

That was too large for the current trainer path, so I created an evenly spaced
subset:

- subset:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-outer-batches-reqmatched-1536.rdkbatch`
- `1536` samples
- `3,607,839,322` bytes on disk

Then I trained only the `384KB` dict, to match the size we were already using:

- `/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dict-384k.zstd`

Holdout result on that batch-shaped corpus:

- improvement vs plain outer zstd: `0.72%`

And the full request-matched outer benchmark came out to:

- outer `zstd` plus retrained dict:
  - `tx_bytes=1,327,196,714`
  - `produce_reqs=3135`

That is:

- `15.04%` smaller than the original wrapped baseline
- only `604,836` bytes better than the previous request-matched outer run
- about `0.039` points of baseline bytes, effectively noise at this stage

So retraining on the exact request-matched cap did not materially change the
conclusion. Payload-dict still wins by a wider margin than the retraining
helped.


### Real Broker Attempt With Outer Dict

The first real-cluster outer-dict attempt failed, but the failure turned out to
be broker configuration, not producer bytes.

The clean single-message retry showed the exact problem in the broker logs:

- `Zstd dictionary support inactive: /tmp/points-tags-outer-batch-dicts-reqmatched-1536/dicts does not exist`
- `Missing zstd dictionary id 978876741 in /tmp/points-tags-outer-batch-dicts-reqmatched-1536/dicts`

So the broker expected a `dicts/` subdirectory under the configured base path.
After creating:

- `/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dicts`

and copying the same producer dict there, a one-message real-cluster replay with
outer `zstd` plus dict succeeded.

After that layout fix, the full real-cluster replay also succeeded with:

- corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-full-records.bin`
- dict:
  `/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dict-384k.zstd`
- replay mode:
  `POINTS_TAGS_REPLAY_MODE=outer-zstd-dict`

Result:

- topic:
  `rdkafkatest_rnd159959115933d973_0209-pt-outer-zstd-dict`
- sent:
  `374531` records
- topic dir bytes:
  `1,296,112,582`
- topic segment `.log` bytes:
  `1,275,092,165`

Compared to the original wrapped baseline:

- baseline segment bytes:
  `1,610,530,085`
- outer `zstd` plus dict segment bytes:
  `1,275,092,165`
- improvement:
  `20.83%`

Compared to the best real payload-dict replay:

- payload-dict segment bytes:
  `1,309,243,277`
- outer `zstd` plus dict segment bytes:
  `1,275,092,165`
- delta:
  `34,151,112` fewer bytes
- improvement over payload-dict:
  `2.61%`

So once the broker can actually load the dictionary, the real-cluster outer path
does beat the real payload-dict path on stored bytes for this dataset.


### Partitioned Dump Replay

I then repeated the outer-dict experiment on a newer, cleaner dump:

- corpus:
  `/home/bits/points-tags-datadog-moon_normal_f88f-0-1079-202603031827.dump`
- format:
  repeated
  `u32 payload_len_le + u32 partition_le + payload bytes`

Important baseline clarification:

- these payload bytes are already current Nicky output, not raw points
- the first payload begins with `03 4a 4b 4e`, which is `NKJ3`
- the first 20 records all had the same `NKJ*` prefix
- so the correct baseline is:
  replay the dump **as-is**, preserving partition, with Kafka
  `compression.type=none`

This is now supported directly by the shared corpus layer and `0209`:

- `CORPUS_FORMAT=partition-dump`
- `POINTS_TAGS_REPLAY_MODE=baseline`

Methodology:

1. Replay the dump as-is to a fresh real-cluster topic, preserving the recorded
   partition for every message.
2. Sum the resulting topic segment `.log` bytes across all broker log dirs.
3. Replay the same dump again using the outer `zstd` plus dict path, also
   preserving partition.
4. Sum those topic segment `.log` bytes the same way.

Commands used:

```bash
make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests test-runner
```

```bash
CORPUS_PATH=/home/bits/points-tags-datadog-moon_normal_f88f-0-1079-202603031827.dump \
CORPUS_FORMAT=partition-dump \
POINTS_TAGS_REPLAY_MODE=baseline \
RDKAFKA_TEST_CONF=/tmp/test-0209-real-kafka.conf \
TESTS=0209 make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests run_seq
```

```bash
CORPUS_PATH=/home/bits/points-tags-datadog-moon_normal_f88f-0-1079-202603031827.dump \
CORPUS_FORMAT=partition-dump \
DICT_PATH=/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dict-384k.zstd \
POINTS_TAGS_REPLAY_MODE=outer-zstd-dict \
OUTER_COMPRESSION_LEVEL=3 \
RDKAFKA_TEST_CONF=/tmp/test-0209-real-kafka.conf \
TESTS=0209 make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests run_seq
```

Segment byte measurement for each topic:

```bash
python3 - <<'PY'
from pathlib import Path
prefix = 'TOPIC_PREFIX_HERE'
total = 0
for root in map(Path, ['/tmp/kafka-logs-0', '/tmp/kafka-logs-1', '/tmp/kafka-logs-2']):
    if not root.exists():
        continue
    for d in root.iterdir():
        if d.is_dir() and d.name.startswith(prefix):
            for f in d.iterdir():
                if f.is_file() and f.suffix == '.log':
                    total += f.stat().st_size
print(total)
PY
```

Replay details:

- messages:
  `833569`
- partitions:
  `1080`

Baseline replay result:

- topic:
  `rdkafkatest_rnd2e2c8b4055a21892_0209-pt-baseline`
- topic partition dirs:
  `1080`
- topic segment `.log` bytes:
  `1,002,623,508`

Outer `zstd` plus dict result:

- topic:
  `rdkafkatest_rnd10a305212721eacb_0209-pt-outer-zstd-dict`
- dict:
  `/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dict-384k.zstd`
- topic partition dirs:
  `1080`
- topic segment `.log` bytes:
  `581,004,388`

Comparison:

- baseline:
  `1,002,623,508`
- outer `zstd` plus dict:
  `581,004,388`
- delta:
  `421,619,120` fewer bytes
- improvement:
  `42.05%`
- ratio:
  `0.5795`

Broker sanity check:

- the successful replay did **not** produce new broker log lines containing:
  `Missing zstd dictionary id`
- and it did **not** produce:
  `Dictionary mismatch`
- and it did **not** produce:
  `Failed to decompress record stream`

So for this newer partition-preserving dump, the real-cluster outer-batch
`zstd` plus dict result is much stronger than the earlier single-partition
segment-slice experiments, and the methodology is finally comparing against the
actual current pipeline output rather than a guessed baseline.


### Same Batch-Trained Dict Used Per Message

To sanity-check whether the `42.05%` result was just the dict being magical on
anything, I reused the **same batch-trained outer dict** in the payload-dict
path on the same partition-preserving dump.

This is intentionally the wrong training shape:

- dict:
  `/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dict-384k.zstd`
- trained on:
  pre-outer-zstd Kafka batch bytes
- replay mode:
  `POINTS_TAGS_REPLAY_MODE=payload-dict`

Command used:

```bash
CORPUS_PATH=/home/bits/points-tags-datadog-moon_normal_f88f-0-1079-202603031827.dump \
CORPUS_FORMAT=partition-dump \
DICT_PATH=/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dict-384k.zstd \
POINTS_TAGS_REPLAY_MODE=payload-dict \
DICT_LEVEL=3 \
RDKAFKA_TEST_CONF=/tmp/test-0209-real-kafka.conf \
TESTS=0209 make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests run_seq
```

Result:

- topic:
  `rdkafkatest_rnd47e82bb55b4d9873_0209-pt-payload-dict`
- topic partition dirs:
  `1080`
- topic segment `.log` bytes:
  `969,001,553`

Compared to the true baseline for the same dump:

- baseline:
  `1,002,623,508`
- payload-dict with batch-trained dict:
  `969,001,553`
- improvement:
  `3.35%`
- ratio:
  `0.9665`

Compared to the correctly matched outer-batch dict result:

- outer `zstd` plus dict:
  `581,004,388`
- payload-dict with batch-trained dict:
  `969,001,553`
- delta:
  `387,997,165` more bytes than outer
- outer is better by:
  `40.04%` relative to this payload-dict result

So the dump experiment is internally consistent:

- the large outer-batch win is not because this dict is universally good
- the same dict is mediocre when applied per message
- training shape really does matter


### Message-Trained Dict On The Same Dump

I then trained a new dict on **individual logical messages** from the same
partition-preserving dump, specifically to compare against the batch-trained
outer dict.

This one is the right training shape for the payload-dict path:

- training corpus:
  `/home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-dump-logical-100k.dump`
- input:
  first `100000` messages from
  `/home/bits/points-tags-datadog-moon_normal_f88f-0-1079-202603031827.dump`
- extracted logical bytes:
  `556,417,860`
- train / holdout:
  `80000 / 20000`
- dict size:
  `393216` bytes (`384KB`)

Training commands:

```bash
/home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/points_tags_logical_extract/points_tags_logical_extract \
  --input-format partition-dump \
  --output-format partition-dump \
  --max-messages 100000 \
  /home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-dump-logical-100k.dump \
  /home/bits/points-tags-datadog-moon_normal_f88f-0-1079-202603031827.dump
```

```bash
/home/bits/go/src/github.com/DataDog/librdkafka/tests/tools/payload_dict/payload_dict_train \
  --input-format partition-dump \
  --sizes 393216 \
  /home/bits/go/src/github.com/DataDog/librdkafka/.tmp/points-tags-dump-logical-100k.dump \
  /tmp/points-tags-message-dicts-dump-384k
```

Training result:

- dict requested:
  `393216`
- dict actual:
  `393216`
- dict id:
  `1716821194`
- holdout improvement vs plain zstd:
  `41.37%`

I then copied that dict into the broker-visible directory under a distinct name:

- producer path:
  `/tmp/points-tags-outer-batch-dicts-reqmatched-1536/message-trained-v1.zstd`
- broker path:
  `/tmp/points-tags-outer-batch-dicts-reqmatched-1536/dicts/message-trained-v1.zstd`

Replay command:

```bash
CORPUS_PATH=/home/bits/points-tags-datadog-moon_normal_f88f-0-1079-202603031827.dump \
CORPUS_FORMAT=partition-dump \
DICT_PATH=/tmp/points-tags-outer-batch-dicts-reqmatched-1536/message-trained-v1.zstd \
POINTS_TAGS_REPLAY_MODE=payload-dict \
DICT_LEVEL=3 \
RDKAFKA_TEST_CONF=/tmp/test-0209-real-kafka.conf \
TESTS=0209 make -C /home/bits/go/src/github.com/DataDog/librdkafka/tests run_seq
```

Result:

- topic:
  `rdkafkatest_rndd4c0c3d55daf469_0209-pt-payload-dict`
- topic partition dirs:
  `1080`
- topic segment `.log` bytes:
  `636,976,123`

Compared to the same true baseline:

- baseline:
  `1,002,623,508`
- payload-dict with message-trained dict:
  `636,976,123`
- improvement:
  `36.47%`
- ratio:
  `0.6353`

Compared to the earlier payload-dict run that used the **wrong** batch-trained
dict:

- payload-dict with batch-trained dict:
  `969,001,553`
- payload-dict with message-trained dict:
  `636,976,123`
- delta:
  `332,025,430` fewer bytes
- improvement over wrong-shape payload run:
  `34.26%`

Compared to the correctly matched outer-batch dict result:

- outer `zstd` plus dict:
  `581,004,388`
- payload-dict with message-trained dict:
  `636,976,123`
- delta:
  `55,971,735` more bytes than outer
- outer is better by:
  `8.79%` relative to this payload-dict result

So on this dump:

- message-trained payload-dict is very strong, `36.47%` smaller than baseline
- outer-batch dict still wins on bytes, but by a much smaller margin than before
- the big gap from the earlier `3.35%` payload result was almost entirely
  training-shape mismatch
