# Apache Cassandra 7.0 HMA (Aerospike-Inspired Hybrid Memory Architecture)

**Docker Hub**: [`duongbkak55/cassandra-hma`](https://hub.docker.com/r/duongbkak55/cassandra-hma)  
**GitHub Repository**: [`duongbkak55/cassandra`](https://github.com/duongbkak55/cassandra)

An Aerospike-inspired storage & execution engine for **Apache Cassandra 7.0** that preserves **100% standard CQL client compatibility** while delivering up to **3.10x Point Read throughput**, **2.28x Mixed Read/Write throughput (93% of Aerospike CE)**, and **3.34x LWT/CAS throughput**.

---

## 1. Docker Image Tags & Versions

| Version | Docker Hub Tags | Git Branch / Release | Included HMA Subsystems |
| :--- | :--- | :--- | :--- |
| **`v7.0.0-hma.3` / `v7.0.0-hma.2`** *(Recommended)* | `duongbkak55/cassandra-hma:latest`<br>`duongbkak55/cassandra-hma:v7.0.0-hma.3`<br>`duongbkak55/cassandra-hma:v7.0.0-hma.2`<br>`duongbkak55/cassandra-hma:7.0.0-hma.2`<br>`duongbkak55/cassandra-hma:phase6` | [`feature/hma-phase6-fast-cas`](https://github.com/duongbkak55/cassandra/tree/feature/hma-phase6-fast-cas)<br>[Release `v7.0.0-hma.3`](https://github.com/duongbkak55/cassandra/releases/tag/v7.0.0-hma.3)<br>[Release `v7.0.0-hma.2`](https://github.com/duongbkak55/cassandra/releases/tag/v7.0.0-hma.2) | **Parts 1–6 + 1M/10M Multi-SSTable Stability**: Off-Heap 64B Primary Index (Atomic Flush Commit + Pre-Sized Shards) + **3-SSTable Direct Jump** + Block Defrag + `O_DIRECT` + Streaming Write Buffer + Bounded 32-Stripe Off-Heap Arena Memtable + **`HMAFastCasEngine` (1-Phase CAS + Seqlock Pre-Image Cache + Full-Row Merge)** |
| **`v7.0.0-hma.1`** | `duongbkak55/cassandra-hma:v7.0.0-hma.1`<br>`duongbkak55/cassandra-hma:7.0.0-hma.1`<br>`duongbkak55/cassandra-hma:phase5` | [`hma-main`](https://github.com/duongbkak55/cassandra/tree/hma-main)<br>[Release `v7.0.0-hma.1`](https://github.com/duongbkak55/cassandra/releases/tag/v7.0.0-hma.1) | **Parts 1–5**: Off-Heap 64B Primary Index + 2-SSTable Base+Delta Jump + Block Defrag + `O_DIRECT` + Streaming Write Buffer + Off-Heap Arena Memtable |

---

## 2. Quick Start (`docker run` & `docker-compose.yml`)

### 2.1. Profile A: Full HMA + Fast-CAS + Off-Heap Arena Memtable (`v7.0.0-hma.2`)
Ideal for high-throughput point reads, mixed updates, and CQL Lightweight Transactions (`UPDATE ... IF ...` / `INSERT ... IF NOT EXISTS`):

```bash
docker run -d \
  --name cassandra-hma \
  --cpus=4 --memory=4g \
  -p 9042:9042 \
  -e MAX_HEAP_SIZE=2G \
  -e HEAP_NEWSIZE=512M \
  -e JVM_EXTRA_OPTS="-Dcassandra.hma.enabled=true -Dcassandra.hma.memtable.auto_enable=true -Dcassandra.hma.fast_cas.enabled=true -Dcassandra.hma.defrag.enabled=true" \
  duongbkak55/cassandra-hma:v7.0.0-hma.2
```

### 2.2. Profile B: Physical NVMe `O_DIRECT` + Single-Write Streaming Buffer
Bypasses Linux OS Page Cache (`O_DIRECT` 4096B aligned I/O) and writes mutations once directly to 8 MiB Off-Heap Streaming Write Buffers (bypassing double-writes to `CommitLog` and Heap `Memtable`):

```bash
docker run -d \
  --name cassandra-hma-nvme \
  --cpus=4 --memory=4g \
  -p 9042:9042 \
  -v /mnt/nvme/cassandra:/var/lib/cassandra \
  -e MAX_HEAP_SIZE=2G \
  -e HEAP_NEWSIZE=512M \
  -e JVM_EXTRA_OPTS="-Dcassandra.hma.enabled=true -Dcassandra.hma.direct_io.enabled=true -Dcassandra.hma.single_write.enabled=true -Dcassandra.hma.swb.block_size_bytes=8388608" \
  duongbkak55/cassandra-hma:v7.0.0-hma.2
```

### 2.3. `docker-compose.yml` Example
```yaml
services:
  cassandra-hma:
    image: duongbkak55/cassandra-hma:v7.0.0-hma.2
    container_name: cassandra-hma
    ports:
      - "9042:9042"
      - "7199:7199"
    environment:
      MAX_HEAP_SIZE: "2G"
      HEAP_NEWSIZE: "512M"
      JVM_EXTRA_OPTS: >-
        -Dcassandra.hma.enabled=true
        -Dcassandra.hma.memtable.auto_enable=true
        -Dcassandra.hma.fast_cas.enabled=true
        -Dcassandra.hma.defrag.enabled=true
        -Dcassandra.hma.defrag.lwm_pct=50
    volumes:
      - cassandra_data:/var/lib/cassandra

volumes:
  cassandra_data:
```

---

## 3. Complete Reference of Newly Added HMA Parameters (`-Dcassandra.hma.*`)

Pass any of the following JVM system properties via `JVM_EXTRA_OPTS` (or in `conf/jvm-server.options`):

### Part 1: 64-Byte Off-Heap Primary Index & Multi-SSTable Direct Offset Jump
| System Property | Default | Version | Description |
| :--- | :---: | :---: | :--- |
| `-Dcassandra.hma.enabled` | `true` | `v7.0.0-hma.1+` | Master switch for the 64-byte Off-Heap Primary Index (`HMAPrimaryIndexManager`) and Direct Offset Jump bypass of Bloom Filters, Partition Summaries, and Partition Indexes. Set `false` to run vanilla Cassandra baseline. |
| `-Dcassandra.hma.index_system_keyspaces` | `false` | `v7.0.0-hma.1+` | Whether to also build Off-Heap Primary Index shards for internal `system*` keyspaces (by default only user keyspaces are indexed). |
| `-Dcassandra.hma.index.shards` | `max(64, CPUs * 16)` | `v7.0.0-hma.2+` | Number of independent `StampedLock` off-heap hash table shards per table (rounded up to a power of 2). Higher shard count (`64`–`256`) minimizes per-shard resize latency at 1M–10M+ keys. |
| `-Dcassandra.hma.index.initial_shard_capacity` | `1024` | `v7.0.0-hma.2+` | Initial slot capacity per shard (`64 bytes/slot`). Set to `32768` (with `64` shards) for zero-resize pre-allocation at 1M keys. |

### Part 2: Block-Level Defragmentation (`inuse_sz` Tracking & Direct-Free)
| System Property | Default | Version | Description |
| :--- | :---: | :---: | :--- |
| `-Dcassandra.hma.defrag.enabled` | `true` | `v7.0.0-hma.1+` | Enables real-time per-SSTable/per-block live byte tracking (`HMASSTableBlockState`), 0-I/O Direct-Free of 0%-live SSTables, and sparse SSTable defragmentation (`HMABlockDefragTask`). |
| `-Dcassandra.hma.defrag.lwm_pct` | `50` | `v7.0.0-hma.1+` | Low-watermark live utilization percentage (`1`–`95`). SSTables whose live ratio `inuse_sz / written_sz * 100` drops below this threshold are prioritized for defragmentation. |
| `-Dcassandra.hma.defrag.block_size_kb` | `1024` | `v7.0.0-hma.1+` | Logical block size in KiB (`1024` = 1 MiB) tracked inside `HMASSTableBlockState` bitmaps. |
| `-Dcassandra.hma.defrag.replace_stcs` | `false` | `v7.0.0-hma.1+` | When `true`, `SizeTieredCompactionStrategy` delegates background compaction selection entirely to `HMABlockCompactionStrategy`. |

### Part 3: Direct I/O (`O_DIRECT`) Kernel Bypass & Async I/O Ring
| System Property | Default | Version | Description |
| :--- | :---: | :---: | :--- |
| `-Dcassandra.hma.direct_io.enabled` | `false` | `v7.0.0-hma.1+` | Enables `ExtendedOpenOption.DIRECT` (`O_DIRECT`) with 4096-byte aligned off-heap buffers (`HMADirectIOEngine` & `DirectChecksummedSequentialWriter`) to bypass the OS Page Cache on NVMe devices. |
| `-Dcassandra.hma.direct_io.async_threads` | `max(4, CPUs)` | `v7.0.0-hma.1+` | Number of worker threads in the asynchronous Direct I/O completion pool. |

### Part 4: Streaming Write Buffer (Single-Write Bypass of CommitLog & Memtable)
| System Property | Default | Version | Description |
| :--- | :---: | :---: | :--- |
| `-Dcassandra.hma.single_write.enabled` | `false` | `v7.0.0-hma.1+` | Routes point mutations directly into lock-free off-heap `HMAStreamingWriteBuffer` rings and flushes them asynchronously straight to SSTables (bypassing `CommitLog` and Heap `Memtable`). |
| `-Dcassandra.hma.swb.block_size_bytes` | `8388608` *(8 MiB)* | `v7.0.0-hma.1+` | Size in bytes of each Off-Heap Streaming Write Buffer block before triggering an async non-blocking SSTable flush. |

### Part 5: Native Off-Heap Arena (`cf_arenax` / FFM Bridge) & Off-Heap Memtable
| System Property | Default | Version | Description |
| :--- | :---: | :---: | :--- |
| `-Dcassandra.hma.memtable.auto_enable` | `false` | `v7.0.0-hma.1+` | Automatically uses `HMAOffHeapMemtable` ($O(1)$ `keyIndex` + native 1 MiB arena slabs) for all user tables without requiring schema `ALTER TABLE ... WITH memtable = 'hma_offheap'`. |
| `-Dcassandra.hma.memtable.max_offheap_mb` | `256` *(256 MiB)* | `v7.0.0-hma.2+` | Hard ceiling in MiB for a single `HMAOffHeapMemtable`'s off-heap arena before signaling an immediate asynchronous flush (`MEMTABLE_LIMIT`). Prevents native OOM during multi-GB bulk loads. |
| `-Dcassandra.hma.memtable.offheap_divisor` | `2` | `v7.0.0-hma.2+` | Scaling divisor mapping off-heap slab allocations into Cassandra's memtable pool accounting (`1` = 1:1, `2` = 2x on-heap threshold). |
| `-Dcassandra.hma.arena.enabled` | `true` | `v7.0.0-hma.1+` | Enables the native 1 MiB slab pool (`HMAOffHeapArenaPool` & 32-stripe `HMAOffHeapArena`). |
| `-Dcassandra.hma.arena.slab_size_bytes` | `1048576` *(1 MiB)* | `v7.0.0-hma.1+` | Size in bytes of each 64-byte cache-line-aligned native memory slab. |
| `-Dcassandra.hma.arena.max_pooled_slabs` | `64` *(64 MiB)* | `v7.0.0-hma.1+` | Maximum number of 1 MiB native slabs retained in the lock-free recycling pool after Memtable/SWB flushes (`256` in `v7.0.0-hma.1`, tuned to `64` in `v7.0.0-hma.2`). |
| `-Dcassandra.hma.arena.use_ffm` | `true` | `v7.0.0-hma.1+` | Uses JDK 21+ Foreign Function & Memory (`java.lang.foreign.Arena.ofShared()`) when available, falling back automatically to `MemoryUtil.allocate()` on JDK 17. |

### Part 6: Partition-Level Fast-CAS Engine (`HMAFastCasEngine`)
| System Property | Default | Version | Description |
| :--- | :---: | :---: | :--- |
| `-Dcassandra.hma.fast_cas.enabled` | `true` | **`v7.0.0-hma.2`** | Enables 1-phase partition-stripe CAS locking (`16,384` stripes + monotonic microsecond clock + `8,192`-slot Seqlock Pre-Image Cache + Full-Row Merge with partition shadow tombstone), bypassing 4-phase `system.paxos` disk round-trips for single-replica/HMA tables. |

---

## 4. CQL Schema Configuration Options (Per-Table)

In addition to global `-Dcassandra.hma.*` flags, you can configure HMA features per table in standard CQL:

### 4.1. Enable `HMAOffHeapMemtable` on a Specific Table (`cassandra.yaml` / CQL)
```sql
CREATE TABLE benchmark.usertable (
    y_id varchar PRIMARY KEY,
    field0 blob,
    cas_gen bigint
) WITH memtable = 'hma_offheap'
  AND compaction = {
    'class': 'org.apache.cassandra.db.compaction.HMABlockCompactionStrategy',
    'defrag_lwm_pct': '50',
    'block_size_kb': '1024',
    'max_defrag_batch': '4'
  };
```

---

## 5. Runtime JMX Control & Metrics (`org.apache.cassandra.io.sstable:type=HMAPrimaryIndex`)

All HMA subsystems can be inspected or toggled dynamically at runtime without restarting Cassandra via JMX MBean `org.apache.cassandra.io.sstable:type=HMAPrimaryIndex`:

- **Runtime Writable Attributes**:
  - `DirectIOEnabled` (`boolean`): Toggle `O_DIRECT` on/off at runtime.
  - `SingleWriteEnabled` (`boolean`): Toggle Streaming Write Buffer (`Single-Write`) on/off at runtime.
  - `FastCasEnabled` (`boolean`, `v7.0.0-hma.2`): Toggle `HMAFastCasEngine` on/off at runtime.
- **Runtime Operations**:
  - `triggerDefrag(String keyspace, String table)`: Immediately run block-level defragmentation / Direct-Free on a table.
- **Key Observability Metrics**:
  - **Part 1**: `TotalIndexedKeys`, `TotalOffHeapMemoryBytes`, `LookupHits`, `LookupMisses`, `HitRate`, `DirectReadBypassCount`.
  - **Part 2**: `TotalTrackedDiskBytes`, `TotalTrackedLiveBytes`, `GlobalLiveUtilizationPct`, `TotalDirectFreedSSTables`, `TotalDefraggedSSTables`, `TotalDefragReclaimedBytes`.
  - **Part 3**: `DirectIOReads`, `DirectIOWrites`, `DirectIOBytesRead`, `DirectIOBytesWritten`.
  - **Part 4**: `StreamingBufferFlushes`, `StreamingBufferBytesFlushed`, `CommitLogBypassedCount`, `MemtableBypassedCount`.
  - **Part 5**: `OffHeapArenaPooledSlabs`, `OffHeapArenaActiveBytes`, `OffHeapArenaTotalBytesAllocated`, `OffHeapArenaSlabHits`, `OffHeapMemtablePutOps`, `OffHeapMemtableDirectLookupHits`.
  - **Part 6 (`v7.0.0-hma.2`)**: `FastCasEnabled`, `FastCasSuccessCount`, `FastCasConditionNotMetCount`.

---

## 6. Benchmark Summary (`4 vCPU, 4 GB RAM, 25,000 keys × 5 KiB across 5 Waves`)

| Workload | Concurrency | Baseline Cassandra 7.0 | HMA `v7.0.0-hma.1` (Parts 1–5) | **HMA `v7.0.0-hma.2` (Parts 1–6)** | **Speedup vs. Baseline** | **Aerospike CE 8.1.1.1** | **% of Aerospike** |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **100% Point Read (`read100`)** | `c=8` | 13,958.2 TPS *(P99 1.41ms)* | 28,022.4 TPS | **29,486.8 TPS** *(P50 0.24ms, P99 0.51ms)* | **2.11x** | 40,064 TPS | **73.6%** |
| **100% Point Read (`read100`)** | `c=32` | 17,812.0 TPS *(P99 8.48ms)* | 50,836.3 TPS | **53,651.4 TPS** *(P50 0.51ms, P99 1.69ms)* | **3.01x** | 75,380 TPS | **71.2%** |
| **100% Point Read (`read100`)** | `c=64` | 19,405.2 TPS *(P99 14.71ms)* | 59,041.2 TPS | **60,073.3 TPS** *(peak **62,228.7**, P99 3.32ms)* | **3.10x** | 85,541 TPS | **70.2% – 72.7%** |
| **Mixed 50/50 (`b1-cas-off`)** | `c=16` | 13,394.5 TPS *(P99 3.94ms)* | 24,578.0 TPS | **25,862.9 TPS** *(P50 0.38ms, P99 1.52ms)* | **1.93x** | 29,984 TPS | **86.3%** |
| **Mixed 50/50 (`b1-cas-off`)** | `c=64` | 14,062.6 TPS *(P99 18.51ms)* | 23,413.7 TPS | **32,121.6 TPS** *(P50 1.17ms, P99 6.52ms)* | **2.28x** | 34,537 TPS | **93.0%** |
| **Mixed 50/50 LWT (`b1-cas-on`)** | `c=8` | 5,298.7 TPS *(P99 4.88ms)* | 6,625.2 TPS | **14,409.5 TPS** *(P50 0.33ms, P99 1.28ms)* | **2.72x** | 18,505 TPS | **77.9%** |
| **Mixed 50/50 LWT (`b1-cas-on`)** | `c=16` | 5,902.0 TPS *(P99 9.30ms)* | 7,940.5 TPS | **19,714.5 TPS** *(P50 0.46ms, P99 2.31ms)* | **3.34x** | 28,118 TPS | **70.1%** |
| **Mixed 50/50 LWT (`b1-cas-on`)** | `c=64` | 6,384.8 TPS *(P99 43.04ms)* | 7,853.9 TPS | **14,336.6 TPS** *(peak **16,812.4**, P99 19.41ms)* | **2.25x – 2.63x** | 34,043 TPS | **42.1% – 49.4%** |

---

## 7. Optimal Sizing & Tuning Guide for 1M Records (`~5 GiB` Dataset) & 10M Records (`~50 GiB` Dataset)

When scaling from small micro-benchmarks (25K keys) to **1,000,000 records × 5 KiB (`~5 GiB` raw payload)** or **10,000,000 records × 5 KiB (`~50 GiB` raw payload)**, memory budgeting across **JVM Heap**, **Off-Heap Primary Index**, **Off-Heap Arena Memtable**, and **Compaction** is critical to achieve zero errors (`0 Other error`, `0 not found`) and low P99 latency.

### 7.1. Memory Budget Breakdown (1M vs. 10M Records)

| Memory Component | Formula / Sizing Rule | **1M Records (`~5 GiB` data)**<br>*(4 GB Constrained Profile)* | **1M Records (`~5 GiB` data)**<br>*(8 GB Recommended Profile)* | **10M Records (`~50 GiB` data)**<br>*(16 GB Recommended Profile)* |
| :--- | :--- | :---: | :---: | :---: |
| **Container / Host RAM (`--memory`)** | Total physical RAM budget | **`4g`** *(minimum)* | **`8g`** *(recommended)* | **`16g`** |
| **JVM Max Heap (`MAX_HEAP_SIZE`)** | `35% – 45%` of container RAM | **`1792M`** *(1.75 GiB)* | **`3G`** *(3.0 GiB)* | **`6G`** *(6.0 GiB)* |
| **JVM Young Gen (`HEAP_NEWSIZE`)** | `~25% – 30%` of `MAX_HEAP_SIZE` | **`512M`** | **`800M`** | **`1536M`** *(1.5 GiB)* |
| **HMA Off-Heap Primary Index** | `64 B × nextPow2(keys / 0.70)` | **`128 MiB`** *(2,097,152 slots)* | **`128 MiB`** *(2,097,152 slots)* | **`1,024 MiB`** *(16,777,216 slots)* |
| **HMA Off-Heap Memtable (`max_offheap_mb`)** | Active + 1 flushing memtable | **`192 MiB`** *(×2 peak = 384 MiB)* | **`256 MiB`** *(×2 peak = 512 MiB)* | **`512 MiB`** *(×2 peak = 1 GiB)* |
| **HMA Off-Heap Arena Pool (`max_pooled_slabs`)** | `max_pooled_slabs × 1 MiB` | **`64 MiB`** *(`64` slabs)* | **`128 MiB`** *(`128` slabs)* | **`256 MiB`** *(`256` slabs)* |
| **Direct ByteBuffers / Netty / Native JVM** | NIO + Thread stacks + Bloom Filters | `~350 MiB` | `~512 MiB` | `~1.0 GiB` |
| **OS Page Cache / Direct I/O Headroom** | Remaining container RAM | `~1.2 GiB` | **`~3.7 GiB`** | **`~6.2 GiB`** |

### 7.2. Recommended `docker run` for 1M Records (`~5 GiB` Dataset)

#### Option 1: 8 GB RAM Container (Recommended for 1M Records × 5 KiB)
Pre-allocates `64` index shards × `32,768` slots (`128 MiB` total off-heap index) so **zero hash-table resizes** occur during the 1M-record load, and caps each off-heap memtable at `256 MiB`:

```bash
docker run -d \
  --name cassandra-hma-1m \
  --cpus=4 --memory=8g --memory-swap=8g \
  -p 9042:9042 \
  -e MAX_HEAP_SIZE=3G \
  -e HEAP_NEWSIZE=800M \
  -e JVM_EXTRA_OPTS="\
    -Dcassandra.hma.enabled=true \
    -Dcassandra.hma.index.shards=64 \
    -Dcassandra.hma.index.initial_shard_capacity=32768 \
    -Dcassandra.hma.memtable.auto_enable=true \
    -Dcassandra.hma.memtable.max_offheap_mb=256 \
    -Dcassandra.hma.memtable.offheap_divisor=2 \
    -Dcassandra.hma.arena.max_pooled_slabs=128 \
    -Dcassandra.hma.fast_cas.enabled=true \
    -Dcassandra.hma.defrag.enabled=true" \
  duongbkak55/cassandra-hma:v7.0.0-hma.2
```

#### Option 2: 4 GB RAM Constrained Container (For 1M Records on 4 GB Limit)
When running inside a strict `--memory=4g` container, reduce `MAX_HEAP_SIZE` to `1792M` and `max_offheap_mb` to `192` so active + flushing off-heap memtables plus the `128 MiB` HMA Primary Index never exceed the 4 GB cgroup limit:

```bash
docker run -d \
  --name cassandra-hma-1m-4g \
  --cpus=4 --memory=4g --memory-swap=4g \
  -p 9042:9042 \
  -e MAX_HEAP_SIZE=1792M \
  -e HEAP_NEWSIZE=512M \
  -e JVM_EXTRA_OPTS="\
    -Dcassandra.hma.enabled=true \
    -Dcassandra.hma.index.shards=64 \
    -Dcassandra.hma.index.initial_shard_capacity=32768 \
    -Dcassandra.hma.memtable.auto_enable=true \
    -Dcassandra.hma.memtable.max_offheap_mb=192 \
    -Dcassandra.hma.memtable.offheap_divisor=2 \
    -Dcassandra.hma.arena.max_pooled_slabs=64 \
    -Dcassandra.hma.fast_cas.enabled=true \
    -Dcassandra.hma.defrag.enabled=true" \
  duongbkak55/cassandra-hma:v7.0.0-hma.2
```

### 7.3. `kvbench` Client Configuration for 1M Records (`conf/cassandra-b1-5k-1m.yaml`)

When benchmarking 1,000,000 records with `kvbench`, pay strict attention to the following **3 rules**:
1. **`run.load.records` MUST equal `keys.count` (`1000000`)**:
   - In the default 10M template (`conf/cassandra-b1-5k.yaml`), `keys.count` is `10000000`. If you only override `run.load.records: 1000000` (or pass `--records 1000000`) without setting `keys.count: 1000000`, `kvbench` will generate random read keys in `[0 .. 9,999,999]` where **90% of keys were never inserted**, resulting in `not found` errors!
2. **Use `requestTimeoutMs: 5000` and `retryPolicy: default` during 5 GiB bulk load**:
   - Loading `1,000,000 × 5 KiB` writes `~5.2 GiB` of SSTables and triggers ~20 Memtable flushes plus L0 $\rightarrow$ L1 Leveled Compactions. Setting `requestTimeoutMs: 1000` with `retryPolicy: none` and `load.concurrency: 256` causes transient driver write timeouts (`Other error`) when a connection hits a flush/GC pause.
   - Recommended settings: `load: { records: 1000000, concurrency: 128 }`, `requestTimeoutMs: 5000`, `pool: { localConnections: 4, maxRequestsPerConnection: 1024 }`, and `retryPolicy: default`.
3. **Wait for L0 Compaction (`nodetool compactionstats`) before starting `phase: run` (Optional for Peak TPS)**:
   - Immediately after loading 5 GiB (`1M` records), running `docker exec cassandra-hma-1m nodetool flush` and waiting until `nodetool compactionstats` reports `pending tasks: 0` ensures steady-state read/write latency from second 0.

Ready-to-use [`conf/cassandra-b1-5k-1m.yaml`](file:///Users/duongnguyen/Projects/cassandra/kvbench/conf/cassandra-b1-5k-1m.yaml):
```yaml
# Optimal 1,000,000-record (5 KiB/record, ~5 GiB dataset) configuration for Cassandra HMA
run:
  name: cassandra-b1-5k-1m
  phase: run
  mode: async
  concurrency: 64
  maxThroughput: 0
  warmup: 30s
  duration: 300s
  reportInterval: 1s
  seed: 42
  maxErrorPct: 1
  load: { records: 1000000, concurrency: 128 }
keys: { count: 1000000, distribution: uniform }
workload:
  schema: subscriber
  mix: { READ: 50, OPERATE_ABM: 50 }
  OPERATE_ABM:
    preRead: true
    casGeneration: true
    maxCasRetries: 5
    steps:
      - { op: PUT_BIN, bin: subscriber }
      - { op: MAP_PUT, bin: balance, count: 2, mode: UPDATE_ONLY }
      - { op: MAP_PUT, bin: session_blob, count: 1, mode: UPSERT }
      - { op: MAP_PUT, bin: timeout_ctl, count: 1, mode: UPSERT }
db:
  type: cassandra
  cassandra:
    contactPoints: ["127.0.0.1:9042"]
    localDc: datacenter1
    keyspace: kvbench_m1
    createSchema: true
    replication: { class: NetworkTopologyStrategy, datacenter1: 1 }
    compaction: LeveledCompactionStrategy
    consistency: { read: LOCAL_ONE, write: LOCAL_ONE, serial: LOCAL_SERIAL }
    requestTimeoutMs: 5000
    pool: { localConnections: 4, maxRequestsPerConnection: 1024 }
    throttler: { type: none, maxConcurrentRequests: 0, maxQueueSize: 0 }
    retryPolicy: default
    speculativeExecution: false
    compression: none
```

### 7.4. Summary of Stability & Correctness Fixes for 1M+ Records (`v7.0.0-hma.2` Patch)

1. **Atomic Index Commit on SSTable Completion (`HMAFlushObserver`, `BigTableReader`, `BtiTableReader`)**:
   - Previously, `HMAFlushObserver` updated `HMAPrimaryIndex` as each partition was being written to an uncommitted SSTable during flush/compaction, and `BigTableReader` skipped live SSTables whenever `ptr.sstableId != hmaSSTableId && !ptr.isMultiSSTable()`. During background compaction at 1M keys, readers could see the new `sstableId` in `HMAPrimaryIndex` before the new SSTable was added to `View.sstables`, causing false `not found` results.
   - **Fixed**: `HMAFlushObserver` now buffers pre-hashed partition pointers in compact 4096-entry chunks and commits them atomically in `complete()`, and `BigTableReader` / `BtiTableReader` no longer perform unverified negative skips.
2. **Compaction Base-SSTable Pointer Transition (`HMAPrimaryIndexShard`, `SinglePartitionReadCommand`)**:
   - Fixed `HMAPrimaryIndexShard.put()` so that when an older base SSTable is compacted into a new SSTable with a higher sequence ID while a newer partial delta SSTable exists, `baseSstableId` and `baseDataOffset` are seamlessly updated to the compacted SSTable. Removed the unsafe `sid > hmaPtr.sstableId` cutoff in `SinglePartitionReadCommand`.
3. **Full-Row Merge & Partial Pre-Read Guard in `HMAFastCasEngine` (`CQL3CasRequest`, `HMAFastCasEngine`, `SinglePartitionReadCommand`)**:
   - Fixed `HMAFastCasEngine.fetchesAllRegularColumns()` so `SelectionColumnFilter` created by `CQL3CasRequest` properly activates both the `STRIPE_CACHE` pre-image hit and the Full-Row Merge + Partition Shadow Tombstone optimization. Added `stoppedEarlyByReduceFilter` guard in `SinglePartitionReadCommand` so single-column `SELECT "gen"` queries that stop early at a delta SSTable never overwrite `STRIPE_CACHE` with an incomplete row.
4. **Bounded Off-Heap Memtable Quota (`HMAOffHeapMemtable`)**:
   - Added `-Dcassandra.hma.memtable.max_offheap_mb=256` and `-Dcassandra.hma.memtable.offheap_divisor=2` so `HMAOffHeapMemtable` flushes predictably during multi-GB loads without triggering container OOM or write timeouts (`Other error`).
