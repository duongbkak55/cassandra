/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * HMA (Hybrid Memory Architecture) Primary Index — Aerospike-inspired off-heap
 * record pointer index for Apache Cassandra.
 *
 * <h2>Design Goals</h2>
 * <ul>
 *   <li>O(1) point-read key resolution via off-heap hash table (no Bloom Filter,
 *       no Partition Summary, no Partition Index traversal)</li>
 *   <li>Fixed 64 bytes per entry (= 1 CPU cache line), aligned for minimal cache misses</li>
 *   <li>Zero JVM GC pressure — all index data lives in native memory managed via
 *       {@link org.apache.cassandra.io.util.Memory}</li>
 *   <li>Last-Write-Wins (LWW) conflict resolution matching Cassandra's timestamp semantics</li>
 *   <li>Sharded hash table with StampedLock per shard for high-concurrency reads</li>
 * </ul>
 *
 * <h2>Module Structure</h2>
 * <dl>
 *   <dt>{@link org.apache.cassandra.io.sstable.format.hma.HMARecordPointer}</dt>
 *   <dd>64-byte record pointer layout (key hash, SSTable ID, offset, length, timestamp, flags)</dd>
 *
 *   <dt>{@link org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndexShard}</dt>
 *   <dd>Single-shard open-addressing hash table with linear probing and StampedLock</dd>
 *
 *   <dt>{@link org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndex}</dt>
 *   <dd>Sharded facade distributing keys across N shards by hash</dd>
 *
 *   <dt>{@link org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndexManager}</dt>
 *   <dd>Singleton manager — one index per Cassandra table (ColumnFamily)</dd>
 *
 *   <dt>{@link org.apache.cassandra.io.sstable.format.hma.HMAFlushObserver}</dt>
 *   <dd>SSTableFlushObserver that populates the index during Memtable flush / compaction</dd>
 *
 *   <dt>{@link org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndexMetrics}</dt>
 *   <dd>JMX metrics bridge (off-heap memory, entry count, per-entry overhead)</dd>
 * </dl>
 *
 * <h2>Memory Budget</h2>
 * <p>Each indexed partition key consumes 64 bytes of off-heap memory.
 * For 1 billion keys: 64B × 10⁹ ÷ 0.70 load factor ≈ 85 GB RAM.
 * For 100 million keys: ≈ 8.5 GB RAM.</p>
 *
 * @see org.apache.cassandra.io.sstable.format.SSTableFormat
 * @see org.apache.cassandra.io.sstable.SSTableFlushObserver
 */
package org.apache.cassandra.io.sstable.format.hma;
