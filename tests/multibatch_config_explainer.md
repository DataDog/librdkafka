# MultiBatch Configuration Guide

## Architecture Overview

MultiBatch changes how librdkafka sends data to Kafka brokers. Understanding the hierarchy of batching is key:

```
┌─────────────────────────────────────────────────────────────┐
│ Producer Client                                             │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐                 │
│  │ Partition 0     │  │ Partition 1     │  ... (N parts)  │
│  │  Message Queue  │  │  Message Queue  │                 │
│  │  [msg][msg]...  │  │  [msg][msg]...  │                 │
│  └────────┬────────┘  └────────┬────────┘                 │
│           │                    │                           │
│           ▼                    ▼                           │
│  ┌─────────────────┐  ┌─────────────────┐                 │
│  │ MessageSet 0    │  │ MessageSet 1    │                 │
│  │ (batch.size,    │  │ (batch.size,    │                 │
│  │  batch.num.msgs)│  │  batch.num.msgs)│                 │
│  └────────┬────────┘  └────────┬────────┘                 │
│           │                    │                           │
│           └──────────┬─────────┘                           │
│                      ▼                                     │
│           ┌──────────────────────┐                         │
│           │   ProduceRequest     │                         │
│           │ (message.max.bytes)  │                         │
│           │ (max.partitions)     │                         │
│           └──────────────────────┘                         │
└─────────────────────────────────────────────────────────────┘
```

## Configuration Hierarchy

### 1. Per-Partition Batching (MessageSet Level)

**`batch.size`** (default: 1,000,000 bytes = 1MB)
- Maximum **uncompressed** size of messages in ONE partition's MessageSet
- Each partition independently batches its messages up to this limit
- Example: With 1000 partitions, you could have up to 1000 MessageSets of 1MB each

**`batch.num.messages`** (default: 10,000)
- Maximum number of messages in ONE partition's MessageSet
- Whichever limit hits first (batch.size or batch.num.messages) stops the batch
- Example: 100 messages × 256 bytes = 25.6KB might trigger batch.size first

**`linger.ms`** (default: 5ms)
- How long to wait for more messages before sending a batch
- Applies to each partition's MessageSet
- Higher values allow more time to fill batches, trading latency for throughput

### 2. ProduceRequest Aggregation (MultiBatch Level)

**`produce.request.max.partitions`** (default: 10)
- Maximum number of partitions (MessageSets) to pack into ONE ProduceRequest
- **THIS IS THE KEY MULTIBATCH SETTING**
- Traditional: 1 partition per request (MultiBatch disabled)
- MultiBatch: Many partitions per request (e.g., 1000)

**`message.max.bytes`** (default: 1,000,000 bytes = 1MB)
- Maximum size of the **entire ProduceRequest** (the "envelope")
- This is the final gating limit for MultiBatch
- Sum of all partition MessageSets (after compression) must fit within this
- Example calculation:
  ```
  1000 partitions × 1MB MessageSet each = 1000MB raw
  With LZ4 compression: ~100MB compressed
  Needs: message.max.bytes ≥ 100MB
  ```

### 3. Global Queue Limits

**`queue.buffering.max.messages`** (default: 100,000)
- Maximum total messages across ALL partitions in producer queue
- Backpressure kicks in when exceeded
- Should be ≥ (num_partitions × batch.num.messages) for full batching

**`queue.buffering.max.kbytes`** (default: 1,048,576 = 1GB)
- Maximum total bytes across ALL partitions in producer queue
- Should be ≥ (num_partitions × batch.size / 1024) for full batching

**`queue.buffering.backpressure.threshold`** (default: 1)
- As fraction of queue.buffering.max.* when backpressure starts
- Lower values (e.g., 0.1) allow queues to fill more before blocking
- Higher values (e.g., 0.9) block producers earlier

## How Limits Interact in MultiBatch

### Scenario: 1000 partitions, 100 messages each (256 bytes/msg)

**Step 1: Per-Partition Batching**
```
Each partition creates a MessageSet:
- 100 messages × 256 bytes = 25,600 bytes (25KB)
- Well under batch.size (1MB) ✓
- Well under batch.num.messages (10,000) ✓
→ Result: 1000 MessageSets of ~25KB each
```

**Step 2: MultiBatch Aggregation**
```
Try to pack partitions into ProduceRequest:
- Total raw size: 1000 × 25KB = 25MB
- With LZ4 compression: ~3-4MB compressed
- Check limits:
  ✗ produce.request.max.partitions = 10 → Can only fit 10 partitions
  ✗ message.max.bytes = 1MB → Can only fit ~30 partitions (compressed)
→ Result: Need to increase BOTH limits
```

**Step 3: After Fixing Limits**
```
Set: produce.request.max.partitions = 10000
     message.max.bytes = 100MB

- 1000 partitions × 25KB each = 25MB raw
- With LZ4 compression: ~3-4MB compressed
- Check limits:
  ✓ produce.request.max.partitions = 10000 → 1000 partitions fit
  ✓ message.max.bytes = 100MB → 4MB fits easily
→ Result: 1 ProduceRequest with all 1000 partitions!
```

## Common Configuration Mistakes

### Mistake 1: Only increasing `produce.request.max.partitions`
```
produce.request.max.partitions = 10000  ✓
message.max.bytes = 1MB (default)       ✗

Problem: Can request 10k partitions but ProduceRequest size limit is still 1MB
Result: Only ~30-50 partitions actually fit per request
```

### Mistake 2: Only increasing `message.max.bytes`
```
produce.request.max.partitions = 10 (default)  ✗
message.max.bytes = 100MB                      ✓

Problem: Have space for large request but limited to 10 partitions
Result: Only 10 partitions per request, wasting the 100MB limit
```

### Mistake 3: Not accounting for compression
```
1000 partitions × 1MB batch.size = 1000MB raw
message.max.bytes = 1000MB

Problem: After compression, might be 100MB, but configured for uncompressed size
Result: Works but wastes configuration space
Tip: Account for compression ratio (LZ4 ~10:1 for repetitive data)
```

### Mistake 4: Insufficient queue limits
```
batch.num.messages = 100,000
queue.buffering.max.messages = 100,000
produce.request.max.partitions = 1000

Problem: 1000 partitions × 100 msgs/batch = 100,000 total messages
Result: Queue fills immediately, can't batch effectively
Solution: queue.buffering.max.messages ≥ 1M (1000 × 1000)
```

## Recommended MultiBatch Configuration

For **maximum batching** with many partitions:

```c
/* Per-partition batching */
test_conf_set(conf, "batch.size", "10000000");           /* 10MB per partition */
test_conf_set(conf, "batch.num.messages", "100000");     /* 100k msgs per partition */
test_conf_set(conf, "linger.ms", "500");                 /* Wait 500ms to fill batches */
test_conf_set(conf, "compression.type", "lz4");          /* Enable compression */

/* MultiBatch aggregation */
test_conf_set(conf, "produce.request.max.partitions", "10000");  /* Pack many partitions */
test_conf_set(conf, "message.max.bytes", "100000000");           /* 100MB ProduceRequest */

/* Global queue limits */
test_conf_set(conf, "queue.buffering.max.messages", "1000000");  /* 1M messages total */
test_conf_set(conf, "queue.buffering.max.kbytes", "102400");     /* 100MB total */
test_conf_set(conf, "queue.buffering.backpressure.threshold", "1");
```

## Verification: How to Tell What Limit You're Hitting

### Check 1: Messages per Request
```
If msgs/req ≈ batch.num.messages:
→ Hitting per-partition message limit

If msgs/req << batch.num.messages:
→ Hitting ProduceRequest size limit (message.max.bytes)
```

### Check 2: Partitions per Request
```
Calculate: partitions_per_request = total_messages / msgs_per_request / num_partitions

If partitions_per_request ≈ produce.request.max.partitions:
→ Hitting partition limit

If partitions_per_request << produce.request.max.partitions:
→ Hitting message.max.bytes (size limit)
```

### Check 3: Statistics
Look at librdkafka statistics:
```json
{
  "brokers": {
    "broker1": {
      "req": {
        "Produce": 100  // Number of ProduceRequests sent
      },
      "outbuf_cnt": 0,  // Should be 0 (not backlogged)
      "waitresp_cnt": 1 // Should be low (not waiting for responses)
    }
  }
}
```

**Good MultiBatch performance:**
- Low ProduceRequest count
- High msgs/req (10k-100k+)
- outbuf_cnt = 0 (not backlogged)

**Bad MultiBatch performance:**
- High ProduceRequest count
- Low msgs/req (<1000)
- outbuf_cnt > 0 (backlogged)

## Summary: The Configuration Stack

```
Global Queue Limits (queue.buffering.max.*)
    ↓ Controls total memory/messages across all partitions

Per-Partition Batching (batch.size, batch.num.messages, linger.ms)
    ↓ Each partition independently batches messages

Compression (compression.type)
    ↓ MessageSets compressed individually

MultiBatch Aggregation (produce.request.max.partitions)
    ↓ Pack multiple partitions into one ProduceRequest

ProduceRequest Size Limit (message.max.bytes)
    ↓ Final gate: compressed size must fit

Network → Broker
```

**Key insight**: MultiBatch works at the **ProduceRequest level**, aggregating already-batched per-partition MessageSets. Both levels must be configured appropriately.
