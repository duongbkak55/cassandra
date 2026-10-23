# Apache Cassandra 7.0 HMA (Aerospike-Inspired Hybrid Memory Architecture)

**Docker Hub**: [`duongbkak55/cassandra-hma`](https://hub.docker.com/r/duongbkak55/cassandra-hma)  
**GitHub Repository**: [`duongbkak55/cassandra`](https://github.com/duongbkak55/cassandra)

An Aerospike-inspired storage & execution engine for **Apache Cassandra 7.0** that preserves **100% standard CQL client compatibility** while delivering up to **3.10x Point Read throughput**, **2.28x Mixed Read/Write throughput (93% of Aerospike CE)**, and **3.34x LWT/CAS throughput**.

---

## 1. Docker Image Tags & Versions

| Version | Docker Hub Tags | Git Branch / Release | Included HMA Subsystems |
| :--- | :--- | :--- | :--- |
| **`v7.0.0-hma.2`** *(Recommended)* | `duongbkak55/cassandra-hma:latest`<br>`duongbkak55/cassandra-hma:v7.0.0-hma.2`<br>`duongbkak55/cassandra-hma:7.0.0-hma.2`<br>`duongbkak55/cassandra-hma:phase6` | [`feature/hma-phase6-fast-cas`](https://github.com/duongbkak55/cassandra/tree/feature/hma-phase6-fast-cas)<br>[Release `v7.0.0-hma.2`](https://github.com/duongbkak55/cassandra/releases/tag/v7.0.0-hma.2) | **Parts 1–6**: Off-Heap 64B Primary Index + **3-SSTable Direct Jump** + Block Defrag + `O_DIRECT` + Streaming Write Buffer + 32-Stripe Off-Heap Arena Memtable + **`HMAFastCasEngine` (1-Phase CAS + Seqlock Pre-Image Cache)** |
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
