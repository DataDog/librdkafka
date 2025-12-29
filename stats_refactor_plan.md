# Stats Callback Refactor: JSON to Typed Structs

## Overview

Replace the JSON-based statistics callback with a typed struct callback for efficient consumption by Rust (via rust-rdkafka) and C/C++ tests.

**Current state:** `stats_cb(rk, char *json, size_t json_len, opaque)` → sprintf-built JSON
**Target state:** `stats_cb(rk, const rd_kafka_stats_t *stats, opaque)` → typed struct

## Motivation

1. **Performance**: Eliminate JSON serialization CPU cost and memory allocation
2. **Rust efficiency**: Zero-copy struct access via FFI instead of JSON parsing
3. **Type safety**: Compile-time field access instead of runtime string parsing
4. **Simplicity**: Tests currently use crude `strstr()` hacks to parse JSON

---

## Design Decisions

### 1. Struct Layout Strategy

**Decision: Pointer + count with single-block allocation**

```c
typedef struct rd_kafka_stats_s {
    uint32_t broker_cnt;
    rd_kafka_broker_stats_t *brokers;  // Points within same allocation
    uint32_t topic_cnt;
    rd_kafka_topic_stats_t *topics;    // Points within same allocation
    // ...
} rd_kafka_stats_t;
```

Rationale:
- Single `malloc` for entire stats tree (cache-friendly, one free)
- Pointers allow variable counts without fixed max limits
- Rust can iterate `brokers[0..broker_cnt]` naturally via FFI

### 2. Average/Histogram Stats

**Decision: Nested struct mirroring internal `rd_avg_t`**

```c
typedef struct rd_kafka_avg_stats_s {
    int64_t min, max, avg, sum, cnt;
    int64_t stddev;
    int64_t p50, p75, p90, p95, p99, p99_99;
    int64_t oor;       // out of range
    int32_t hdrsize;
} rd_kafka_avg_stats_t;
```

Used for: `rtt`, `int_latency`, `throttle`, `batchsize`, `batchcnt`, etc.

### 3. String Fields

**Decision: Fixed-size char arrays**

```c
char name[RD_KAFKA_STATS_NAME_MAX];      // 256
char nodename[RD_KAFKA_STATS_NAME_MAX];
```

Rationale:
- Simple FFI (no pointer lifetime issues)
- Broker/topic names are bounded in practice
- Rust sees `[c_char; 256]`, can convert to `&str`

### 4. State Fields

**Decision: Use existing enums where available, int32_t for FFI safety**

```c
int32_t state;  // rd_kafka_broker_state_t value
int32_t fetch_state;  // rd_kafka_fetch_state_t value
```

Rust can match on the integer values.

### 5. API Approach

**Decision: Replace existing callback signature (breaking change acceptable for fork)**

```c
// Old (remove)
void rd_kafka_conf_set_stats_cb(
    rd_kafka_conf_t *conf,
    int (*stats_cb)(rd_kafka_t *rk, char *json, size_t json_len, void *opaque));

// New
void rd_kafka_conf_set_stats_cb(
    rd_kafka_conf_t *conf,
    void (*stats_cb)(rd_kafka_t *rk, const rd_kafka_stats_t *stats, void *opaque));
```

Note: Return type changes from `int` to `void` (no ownership transfer needed).

### 6. Event API

**Decision: Update `rd_kafka_event_stats()` to return struct pointer**

```c
// Old (remove)
const char *rd_kafka_event_stats(rd_kafka_event_t *rkev);

// New
const rd_kafka_stats_t *rd_kafka_event_stats(rd_kafka_event_t *rkev);
```

### 7. Memory Lifetime

- Library allocates stats struct before callback
- Callback receives `const` pointer (read-only)
- Library frees after callback returns
- **Rust must copy any data it needs during callback** (cannot retain pointers)

---

## Struct Definitions

### Core Types

```c
// rdkafka_stats.h

#define RD_KAFKA_STATS_NAME_MAX 256

typedef struct rd_kafka_avg_stats_s {
    int64_t min;
    int64_t max;
    int64_t avg;
    int64_t sum;
    int64_t cnt;
    int64_t stddev;
    int64_t p50;
    int64_t p75;
    int64_t p90;
    int64_t p95;
    int64_t p99;
    int64_t p99_99;
    int64_t oor;
    int32_t hdrsize;
    int32_t _pad;  // Alignment padding
} rd_kafka_avg_stats_t;
```

### Partition Stats

```c
typedef struct rd_kafka_partition_stats_s {
    int32_t partition;
    int32_t broker_id;
    int32_t leader;

    int8_t desired;
    int8_t unknown;
    int8_t _pad[2];

    // Producer queues
    int32_t msgq_cnt;
    int64_t msgq_bytes;
    int32_t xmit_msgq_cnt;
    int64_t xmit_msgq_bytes;

    // Consumer
    int32_t fetchq_cnt;
    int64_t fetchq_size;
    int32_t fetch_state;

    // Offsets
    int64_t query_offset;
    int64_t next_offset;
    int64_t app_offset;
    int64_t stored_offset;
    int32_t stored_leader_epoch;
    int64_t committed_offset;
    int32_t committed_leader_epoch;
    int64_t eof_offset;
    int64_t lo_offset;
    int64_t hi_offset;
    int64_t ls_offset;
    int64_t consumer_lag;
    int64_t consumer_lag_stored;
    int32_t leader_epoch;

    // Counters
    int64_t txmsgs;
    int64_t txbytes;
    int64_t rxmsgs;
    int64_t rxbytes;
    int64_t msgs;            // Total messages (produced or consumed)
    int64_t rx_ver_drops;    // Dropped outdated messages
    int64_t msgs_inflight;
    int64_t next_ack_seq;
    int64_t next_err_seq;
    int64_t acked_msgid;
} rd_kafka_partition_stats_t;
```

### Topic Stats

```c
typedef struct rd_kafka_topic_stats_s {
    char name[RD_KAFKA_STATS_NAME_MAX];
    int64_t age_us;
    int64_t metadata_age_us;

    rd_kafka_avg_stats_t batchsize;
    rd_kafka_avg_stats_t batchcnt;

    uint32_t partition_cnt;
    uint32_t _pad;
    rd_kafka_partition_stats_t *partitions;
} rd_kafka_topic_stats_t;
```

### Broker Stats

```c
typedef struct rd_kafka_broker_toppar_ref_s {
    char topic[RD_KAFKA_STATS_NAME_MAX];
    int32_t partition;
    int32_t _pad;
} rd_kafka_broker_toppar_ref_t;

typedef struct rd_kafka_broker_stats_s {
    char name[RD_KAFKA_STATS_NAME_MAX];
    int32_t nodeid;
    char nodename[RD_KAFKA_STATS_NAME_MAX];
    char source[32];
    int32_t state;
    int64_t stateage_us;

    // Buffers
    int32_t outbuf_cnt;
    int32_t outbuf_msg_cnt;
    int32_t waitresp_cnt;
    int32_t waitresp_msg_cnt;

    // Counters
    int64_t tx;
    int64_t tx_bytes;
    int64_t tx_errs;
    int64_t tx_retries;
    int64_t tx_idle_us;
    int64_t req_timeouts;
    int64_t rx;
    int64_t rx_bytes;
    int64_t rx_errs;
    int64_t rx_idle_us;
    int64_t rx_corriderrs;
    int64_t rx_partial;
    int64_t zbuf_grow;
    int64_t buf_grow;
    int64_t wakeups;
    int64_t connects;
    int64_t disconnects;

    // Latencies
    rd_kafka_avg_stats_t int_latency;
    rd_kafka_avg_stats_t outbuf_latency;
    rd_kafka_avg_stats_t rtt;
    rd_kafka_avg_stats_t throttle;

    // Producer request stats
    rd_kafka_avg_stats_t produce_partitions;
    rd_kafka_avg_stats_t produce_messages;
    rd_kafka_avg_stats_t produce_reqsize;
    rd_kafka_avg_stats_t produce_fill;
    rd_kafka_avg_stats_t batch_wait;

    // Request counts (indexed by RD_KAFKAP_* api key)
    int64_t req_counts[64];  // RD_KAFKAP__NUM is ~60

    // Toppars on this broker
    uint32_t toppar_cnt;
    uint32_t _pad;
    rd_kafka_broker_toppar_ref_t *toppars;
} rd_kafka_broker_stats_t;
```

### Consumer Group Stats

```c
typedef struct rd_kafka_cgrp_stats_s {
    int32_t state;
    int64_t stateage_us;
    int32_t join_state;
    int64_t rebalance_age_us;
    int32_t rebalance_cnt;
    char rebalance_reason[256];
    int32_t assignment_size;
    int32_t _pad;
} rd_kafka_cgrp_stats_t;
```

### EOS Stats

```c
typedef struct rd_kafka_eos_stats_s {
    int32_t idemp_state;
    int64_t idemp_stateage_us;
    int32_t txn_state;
    int64_t txn_stateage_us;
    int8_t txn_may_enq;
    int8_t _pad[3];
    int64_t producer_id;
    int16_t producer_epoch;
    int16_t _pad2;
    int32_t epoch_cnt;
} rd_kafka_eos_stats_t;
```

### Top-Level Stats

```c
typedef struct rd_kafka_stats_s {
    // Identification
    char name[RD_KAFKA_STATS_NAME_MAX];
    char client_id[RD_KAFKA_STATS_NAME_MAX];
    int32_t type;  // RD_KAFKA_PRODUCER or RD_KAFKA_CONSUMER
    int32_t _pad;

    // Timestamps
    int64_t ts_us;        // Monotonic timestamp (microseconds)
    int64_t time_sec;     // Unix epoch (seconds)
    int64_t age_us;       // Client age

    // Global state
    int32_t replyq;
    uint32_t msg_cnt;
    uint64_t msg_size;
    uint32_t msg_max;
    uint64_t msg_size_max;
    int32_t simple_cnt;
    int32_t metadata_cache_cnt;

    // Totals (aggregated across brokers)
    int64_t tx;
    int64_t tx_bytes;
    int64_t rx;
    int64_t rx_bytes;
    int64_t txmsgs;
    int64_t txmsg_bytes;
    int64_t rxmsgs;
    int64_t rxmsg_bytes;

    // Brokers
    uint32_t broker_cnt;
    uint32_t _pad2;
    rd_kafka_broker_stats_t *brokers;

    // Topics
    uint32_t topic_cnt;
    uint32_t _pad3;
    rd_kafka_topic_stats_t *topics;

    // Consumer group (optional)
    int8_t has_cgrp;
    int8_t _pad4[7];
    rd_kafka_cgrp_stats_t cgrp;

    // EOS (optional)
    int8_t has_eos;
    int8_t _pad5[7];
    rd_kafka_eos_stats_t eos;

    // Fatal error (optional)
    int8_t has_fatal;
    int8_t _pad6[3];
    int32_t fatal_err;
    char fatal_reason[512];
    int32_t fatal_cnt;
    int32_t _pad7;
} rd_kafka_stats_t;
```

---

## Memory Allocation Strategy

### Single-Block Allocation

Compute total size needed, allocate once:

```c
rd_kafka_stats_t *rd_kafka_stats_new(rd_kafka_t *rk) {
    size_t total_size = sizeof(rd_kafka_stats_t);

    // Count brokers, topics, partitions
    int broker_cnt = count_brokers(rk);
    int topic_cnt = count_topics(rk);
    int total_partitions = count_all_partitions(rk);
    int total_toppars = count_all_broker_toppars(rk);

    total_size += broker_cnt * sizeof(rd_kafka_broker_stats_t);
    total_size += topic_cnt * sizeof(rd_kafka_topic_stats_t);
    total_size += total_partitions * sizeof(rd_kafka_partition_stats_t);
    total_size += total_toppars * sizeof(rd_kafka_broker_toppar_ref_t);

    // Single allocation
    char *buf = rd_malloc(total_size);
    rd_kafka_stats_t *stats = (rd_kafka_stats_t *)buf;
    char *ptr = buf + sizeof(rd_kafka_stats_t);

    // Layout arrays
    stats->brokers = (rd_kafka_broker_stats_t *)ptr;
    stats->broker_cnt = broker_cnt;
    ptr += broker_cnt * sizeof(rd_kafka_broker_stats_t);

    stats->topics = (rd_kafka_topic_stats_t *)ptr;
    stats->topic_cnt = topic_cnt;
    ptr += topic_cnt * sizeof(rd_kafka_topic_stats_t);

    // ... continue laying out partition arrays etc.

    return stats;
}

void rd_kafka_stats_destroy(rd_kafka_stats_t *stats) {
    rd_free(stats);  // Single free
}
```

---

## Implementation Phases

### Phase 1: Add Struct Definitions

**Files:**
- Create `src/rdkafka_stats.h` - All public struct definitions

**Tasks:**
1. Define all structs as specified above
2. Add to `rdkafka.h` includes
3. Ensure proper alignment/padding for cross-platform FFI
4. Add static asserts for struct sizes (detect padding issues)

### Phase 2: Create Stats Population Logic

**Files:**
- Create `src/rdkafka_stats.c` - Population functions

**Tasks:**
1. Implement `rd_kafka_stats_new()` - allocation + size calculation
2. Implement `rd_kafka_stats_populate()` - fill struct from internal state
3. Implement `rd_kafka_stats_destroy()` - cleanup
4. Extract logic from `rd_kafka_stats_emit_all()` into struct-filling code
5. Helper: `rd_kafka_avg_stats_from_avg()` - convert `rd_avg_t` to `rd_kafka_avg_stats_t`

**Key function:**
```c
static void rd_kafka_stats_populate_broker(rd_kafka_broker_stats_t *dst,
                                            rd_kafka_broker_t *rkb,
                                            rd_ts_t now);

static void rd_kafka_stats_populate_topic(rd_kafka_topic_stats_t *dst,
                                           rd_kafka_topic_t *rkt,
                                           rd_ts_t now);

static void rd_kafka_stats_populate_partition(rd_kafka_partition_stats_t *dst,
                                               rd_kafka_toppar_t *rktp,
                                               rd_ts_t now);
```

### Phase 3: Update Callback Mechanism

**Files:**
- `src/rdkafka_conf.h` - Update `stats_cb` type
- `src/rdkafka_conf.c` - Update `rd_kafka_conf_set_stats_cb()`
- `src/rdkafka_op.h` - Update `RD_KAFKA_OP_STATS` payload
- `src/rdkafka.c` - Update timer callback

**Tasks:**
1. Change callback signature in conf struct
2. Update `rd_kafka_conf_set_stats_cb()` to accept new signature
3. Change op payload from `json/json_len` to `rd_kafka_stats_t *`
4. Update `rd_kafka_stats_emit_tmr_cb()` to:
   - Allocate stats struct
   - Populate it
   - Create op with struct pointer
   - Free after callback returns
5. Update `rd_kafka_poll()` / event handling path

### Phase 4: Update Event API

**Files:**
- `src/rdkafka.h` - Update `rd_kafka_event_stats()` signature
- `src/rdkafka_event.c` - Update implementation

**Tasks:**
1. Change return type from `const char *` to `const rd_kafka_stats_t *`
2. Update event struct to hold stats pointer
3. Handle memory ownership (event owns stats, freed on event destroy)

### Phase 5: Update Tests

**Files and changes:**

| File | Change |
|------|--------|
| `tests/0006-symbols.c` | Update symbol check |
| `tests/0025-timers.c` | Minimal - callback signature only |
| `tests/0055-producer_latency.c` | Read `stats->brokers[i].wakeups` instead of strstr |
| `tests/0062-stats_event.c` | Update for new event API |
| `tests/0200-multibatch_benchmark.c` | Read struct fields directly |
| `tests/0202-adaptive_batching_baseline.c` | Read struct fields directly |
| `tests/test.c` | Remove JSON file writing (or add debug dump function) |

**Example transformation (0055-producer_latency.c):**

```c
// Before
static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
    const char *t = json;
    int total = 0;
    while ((t = strstr(t, "\"wakeups\":"))) {
        t += strlen("\"wakeups\":");
        total += strtol(t, NULL, 0);
    }
    tot_wakeups = total;
    return 0;
}

// After
static void stats_cb(rd_kafka_t *rk, const rd_kafka_stats_t *stats, void *opaque) {
    int total = 0;
    for (uint32_t i = 0; i < stats->broker_cnt; i++) {
        total += stats->brokers[i].wakeups;
    }
    tot_wakeups = total;
}
```

### Phase 6: Remove JSON Code

**Files:**
- `src/rdkafka.c` - Remove JSON emission functions

**Tasks:**
1. Delete `_st_printf` macro
2. Delete `struct _stats_emit`
3. Delete `rd_kafka_stats_emit_avg()` (JSON version)
4. Delete `rd_kafka_stats_emit_toppar()` (JSON version)
5. Delete `rd_kafka_stats_emit_broker_reqs()` (JSON version)
6. Delete JSON building code from `rd_kafka_stats_emit_all()`
7. Clean up any JSON-related includes

### Phase 7: Update rust-rdkafka

**Files:** (in ~/dd/rust-rdkafka/)
- Update bindgen configuration to include new structs
- Update stats callback wrapper
- Remove JSON parsing code
- Update metrics exposure to read from structs

---

## Testing Strategy

### Unit Tests
- Verify struct sizes match expected (static asserts)
- Verify alignment is correct
- Test stats population with mock data

### Integration Tests
- Run existing test suite with new API
- Verify all tests pass
- Compare metric values between old JSON and new struct (during development)

### Performance Tests
- Benchmark stats emission time (old vs new)
- Measure memory allocation (old vs new)
- Profile CPU usage during high-frequency stats

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Struct size changes break Rust bindings | Version field or regenerate bindings on any change |
| Alignment issues across platforms | Use explicit padding, static asserts |
| Missing stats fields | Diff JSON output against struct fields systematically |
| Debug difficulty without JSON | Add optional `rd_kafka_stats_dump()` for development |
| rust-rdkafka update coordination | Update both repos in single PR |

---

## Optional: Debug JSON Dump

Keep a debug-only function for development:

```c
#ifdef RD_KAFKA_STATS_DEBUG
char *rd_kafka_stats_to_json(const rd_kafka_stats_t *stats);
#endif
```

Not part of public API, just for debugging during development.

---

## File Summary

### New Files
- `src/rdkafka_stats.h` - Public struct definitions
- `src/rdkafka_stats.c` - Stats allocation and population

### Modified Files
- `src/rdkafka.h` - Updated callback signature, event API
- `src/rdkafka.c` - Remove JSON code, use new stats functions
- `src/rdkafka_conf.h` - Updated callback type
- `src/rdkafka_conf.c` - Updated set_stats_cb
- `src/rdkafka_op.h` - Updated op payload
- `src/rdkafka_event.c` - Updated event stats
- `src/CMakeLists.txt` / `src/Makefile` - Add new source file
- `tests/0006-symbols.c`
- `tests/0025-timers.c`
- `tests/0055-producer_latency.c`
- `tests/0062-stats_event.c`
- `tests/0200-multibatch_benchmark.c`
- `tests/0202-adaptive_batching_baseline.c`
- `tests/test.c`

---

## Estimated Scope

| Phase | Files | Complexity |
|-------|-------|------------|
| 1. Struct definitions | 1 new | Low |
| 2. Population logic | 1 new | Medium - extract from existing |
| 3. Callback mechanism | 4 modified | Medium |
| 4. Event API | 2 modified | Low |
| 5. Update tests | 7 modified | Low - mechanical |
| 6. Remove JSON | 1 modified | Low - deletion |
| 7. rust-rdkafka | external repo | Medium |

Total: ~2 new files, ~15 modified files

---

## rust-rdkafka Integration Analysis

### Current Architecture (~/dd/rust-rdkafka/)

**Stats flow:**
```
librdkafka (C)              rust-rdkafka (Rust)
─────────────────           ───────────────────────────────────────────
rd_kafka_event_stats()  →   handle_stats_event()
returns const char* JSON     │
                             ├─ CStr::from_ptr() → &[u8]
                             ├─ context.stats_raw(json_bytes)
                             │    └─ serde_json::from_slice()
                             │         → Statistics struct
                             └─ context.stats(statistics)
```

**Key files:**
- `src/statistics.rs` - Rust `Statistics` struct (serde-deserializable)
- `src/client.rs:375` - `handle_stats_event()` receives JSON
- `src/client.rs:92-107` - `ClientContext` trait with `stats()` and `stats_raw()`

### Current Rust Statistics Structure

```rust
pub struct Statistics {
    pub brokers: HashMap<String, Broker>,    // keyed by "host:port/id"
    pub topics: HashMap<String, Topic>,      // keyed by topic name
    pub cgrp: Option<ConsumerGroup>,
    pub eos: Option<ExactlyOnceSemantics>,
    // ... scalar fields
}

pub struct Broker {
    pub toppars: HashMap<String, TopicPartition>,  // keyed by "topic-partition"
    pub req: HashMap<String, i64>,                 // keyed by request type name
    pub int_latency: Option<Window>,
    pub rtt: Option<Window>,
    // ...
}

pub struct Topic {
    pub partitions: HashMap<i32, Partition>,  // keyed by partition id
    // ...
}
```

### Structural Differences

| Aspect | C Struct (Planned) | Rust Current | Migration |
|--------|-------------------|--------------|-----------|
| Brokers | `rd_kafka_broker_stats_t brokers[]` | `HashMap<String, Broker>` | Build HashMap from array using `name` field |
| Topics | `rd_kafka_topic_stats_t topics[]` | `HashMap<String, Topic>` | Build HashMap from array using `name` field |
| Partitions | `rd_kafka_partition_stats_t partitions[]` | `HashMap<i32, Partition>` | Build HashMap from array using `partition` field |
| Toppars | `rd_kafka_broker_toppar_ref_t toppars[]` | `HashMap<String, TopicPartition>` | Build HashMap, key = `"{topic}-{partition}"` |
| Req counts | `int64_t req_counts[64]` | `HashMap<String, i64>` | Convert index → API name string |

### Migration Strategy for rust-rdkafka

**Phase 1: Keep Rust API stable (recommended initial approach)**

```rust
// client.rs - updated handle_stats_event
fn handle_stats_event(&self, event: *mut RDKafkaEvent) {
    // NEW: Get typed struct instead of JSON
    let stats_ptr = unsafe { rdsys::rd_kafka_event_stats_typed(event) };
    if stats_ptr.is_null() {
        return;
    }
    let c_stats = unsafe { &*stats_ptr };

    // Convert C struct to Rust Statistics (builds HashMaps)
    let statistics = Statistics::from_native(c_stats);
    self.context().stats(statistics);
}
```

**New conversion impl in statistics.rs:**
```rust
impl Statistics {
    pub fn from_native(c: &rd_kafka_stats_t) -> Self {
        Statistics {
            name: cstr_to_string(&c.name),
            brokers: c.brokers_slice()
                .iter()
                .map(|b| (cstr_to_string(&b.name), Broker::from_native(b)))
                .collect(),
            topics: c.topics_slice()
                .iter()
                .map(|t| (cstr_to_string(&t.name), Topic::from_native(t)))
                .collect(),
            cgrp: if c.has_cgrp != 0 {
                Some(ConsumerGroup::from_native(&c.cgrp))
            } else {
                None
            },
            // ...
        }
    }
}
```

**Benefits:**
- No breaking changes for Rust users
- `Statistics` struct stays the same
- HashMaps preserved for lookup convenience
- Much faster than JSON parsing (no serde, no string allocation for every field)

**Phase 2 (optional): Add zero-copy API**

For users who want maximum performance:
```rust
pub trait ClientContext {
    // Existing - receives owned Rust struct
    fn stats(&self, statistics: Statistics) { ... }

    // NEW - receives reference to C struct (zero-copy)
    fn stats_native(&self, statistics: &rd_kafka_stats_t) {
        // Default: convert and call stats()
        self.stats(Statistics::from_native(statistics));
    }
}
```

### Fields to Add to C Struct

Comparing Rust `statistics.rs` against planned C structs, these fields are missing:

**Partition stats:**
```c
// Add to rd_kafka_partition_stats_s:
int64_t msgs;          // Total messages (producer) or consumed (consumer)
int64_t rx_ver_drops;  // Dropped outdated messages
```

**Already have but verify naming:**
- `stored_leader_epoch` ✓
- `committed_leader_epoch` ✓

### Request Count Conversion

The C struct uses `int64_t req_counts[64]` indexed by `RD_KAFKAP_*` enum.
Rust expects `HashMap<String, i64>` with names like `"Produce"`, `"Fetch"`.

**Solution:** Add helper function in librdkafka or rust-rdkafka:
```c
// Already exists in librdkafka:
const char *rd_kafka_ApiKey2str(int16_t apikey);
```

Rust conversion:
```rust
fn req_counts_to_hashmap(counts: &[i64; 64]) -> HashMap<String, i64> {
    counts.iter()
        .enumerate()
        .filter(|(_, &v)| v > 0)  // Skip zero counts
        .map(|(i, &v)| {
            let name = unsafe {
                CStr::from_ptr(rdsys::rd_kafka_ApiKey2str(i as i16))
            };
            (name.to_string_lossy().into_owned(), v)
        })
        .collect()
}
```

### Updated rust-rdkafka Changes

**Files to modify:**

| File | Changes |
|------|---------|
| `rdkafka-sys/build.rs` | Include new stats header in bindgen |
| `rdkafka-sys/src/bindings.rs` | (auto-generated) New struct bindings |
| `src/statistics.rs` | Add `from_native()` conversion methods |
| `src/client.rs` | Update `handle_stats_event()` to use typed struct |

**Deprecations:**
- `stats_raw(&self, &[u8])` - deprecate, keep for compatibility
- `rd_kafka_event_stats()` returning `const char*` - remove from bindings

### Risk: API Key Name Stability

The request type names come from `rd_kafka_ApiKey2str()`. If librdkafka changes these strings, Rust users who match on them could break.

**Mitigation:** Document that request type names follow Kafka protocol naming and are stable.

### Testing Strategy for rust-rdkafka

1. **Unit test:** Parse example C struct, verify matches expected Rust Statistics
2. **Integration test:** Run existing stats tests, verify same values as JSON path
3. **Benchmark:** Compare JSON parsing vs struct conversion performance
