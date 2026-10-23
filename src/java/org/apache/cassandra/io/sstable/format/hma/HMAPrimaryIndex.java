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
package org.apache.cassandra.io.sstable.format.hma;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.FBUtilities;

/**
 * High-performance Sharded Off-Heap Primary Key Index with Aerospike-style Block-Level Defragmentation tracking.
 *
 * <p>Inspired by Aerospike's Hybrid Memory Architecture (HMA) and SSD Block Defragmenter
 * ({@code ssd_wblock_state.inuse_sz} and {@code defrag_wblock_q}), this index maintains:
 * <ul>
 *   <li>Fixed 64-byte record pointers in off-heap native memory for O(1) point reads</li>
 *   <li>Per-SSTable and per-1MB-block live byte utilization ({@link HMASSTableBlockState})</li>
 *   <li>Lock-free {@code defragQueue} when an SSTable's live ratio drops below {@code defragLwmPct}</li>
 * </ul>
 */
public final class HMAPrimaryIndex implements AutoCloseable
{
    private static final int DEFAULT_SHARD_COUNT = Math.max(16, FBUtilities.getAvailableProcessors() * 4);
    public static final int DEFAULT_DEFRAG_LWM_PCT = Integer.getInteger("cassandra.hma.defrag.lwm_pct", 50);
    public static final int DEFAULT_BLOCK_SIZE_BYTES = Integer.getInteger("cassandra.hma.defrag.block_size_kb", 1024) * 1024;

    private final HMAPrimaryIndexShard[] shards;
    private final int shardMask;
    private final ThreadLocal<long[]> threadLocalHashBuffer = ThreadLocal.withInitial(() -> new long[2]);
    private final LongAdder lookupCount = new LongAdder();
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder directReadBypassCount = new LongAdder();

    // Aerospike-style Block-Level Defragmentation state (ssd_wblock_state + defrag_wblock_q)
    private final ConcurrentHashMap<Long, HMASSTableBlockState> blockStates = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Long> defragQueue = new ConcurrentLinkedQueue<>();
    private volatile int defragLwmPct = DEFAULT_DEFRAG_LWM_PCT;
    private final int blockSizeBytes = DEFAULT_BLOCK_SIZE_BYTES;

    // Defragmentation telemetry counters
    private final LongAdder directFreedSSTableCount = new LongAdder();
    private final LongAdder defragRunCount = new LongAdder();
    private final LongAdder defraggedSSTableCount = new LongAdder();
    private final LongAdder defragMovedRecordCount = new LongAdder();
    private final LongAdder defragSkippedRecordCount = new LongAdder();
    private final LongAdder defragReclaimedByteCount = new LongAdder();

    public HMAPrimaryIndex()
    {
        this(DEFAULT_SHARD_COUNT, 1024);
    }

    public HMAPrimaryIndex(int numShards, int initialCapacityPerShard)
    {
        int count = 1;
        while (count < numShards)
            count <<= 1;

        this.shards = new HMAPrimaryIndexShard[count];
        this.shardMask = count - 1;

        for (int i = 0; i < count; i++)
        {
            this.shards[i] = new HMAPrimaryIndexShard(initialCapacityPerShard);
        }
    }

    private int selectShard(long hashHi, long hashLo)
    {
        return (int) (hashHi ^ (hashHi >>> 32) ^ hashLo) & shardMask;
    }

    /**
     * Store or update a record pointer for the given partition key, automatically updating
     * {@code inuse_sz} on both the new SSTable and any superseded SSTable.
     */
    public boolean put(DecoratedKey key,
                       long sstableId,
                       long dataOffset,
                       int dataLength,
                       long timestamp,
                       int generation,
                       byte flags)
    {
        long[] hash = threadLocalHashBuffer.get();
        HMARecordPointer.computeKeyHash(key, hash);
        int shardIdx = selectShard(hash[0], hash[1]);
        return shards[shardIdx].put(hash[0], hash[1], sstableId, dataOffset, dataLength, timestamp, generation, flags, this);
    }

    /**
     * Convenience method to index a live SSTable partition record as a complete partition write.
     */
    public boolean put(DecoratedKey key,
                       long sstableId,
                       long dataOffset,
                       int dataLength,
                       long timestamp)
    {
        return put(key,
                   sstableId,
                   dataOffset,
                   dataLength,
                   timestamp,
                   0,
                   (byte) (HMARecordPointer.FLAG_IN_USE | HMARecordPointer.FLAG_COMPLETE_OVERWRITE));
    }

    /**
     * Callback invoked by {@link HMAPrimaryIndexShard} when a partition record is written to an SSTable.
     */
    void onRecordWritten(long sstableId, long dataOffset, int dataLength, boolean isLive)
    {
        if (sstableId <= 0)
            return;
        HMASSTableBlockState state = blockStates.computeIfAbsent(
            sstableId, id -> new HMASSTableBlockState(id, blockSizeBytes));
        state.onRecordWritten(dataOffset, dataLength, isLive);
        maybeEnqueueForDefrag(state);
    }

    /**
     * Callback invoked by {@link HMAPrimaryIndexShard} when an existing partition record is overwritten
     * or deleted by a newer write, decrementing {@code inuse_sz} on the old SSTable and write block.
     */
    void onRecordSuperseded(long oldSstableId, long oldDataOffset, int oldDataLength)
    {
        if (oldSstableId <= 0)
            return;
        HMASSTableBlockState state = blockStates.get(oldSstableId);
        if (state != null)
        {
            state.onRecordSuperseded(oldDataOffset, oldDataLength);
            maybeEnqueueForDefrag(state);
        }
    }

    /**
     * Mark an SSTable as sealed (immutable) after flush/compaction/import completes.
     * If its utilization is already below {@code defragLwmPct}, enqueue it for defragmentation.
     */
    public void markSSTableSealed(long sstableId)
    {
        if (sstableId <= 0)
            return;
        HMASSTableBlockState state = blockStates.get(sstableId);
        if (state != null)
        {
            state.markSealed();
            maybeEnqueueForDefrag(state);
        }
    }

    private void maybeEnqueueForDefrag(HMASSTableBlockState state)
    {
        if (state.needsDefrag(defragLwmPct) && state.markEnqueuedForDefrag())
        {
            defragQueue.offer(state.getSstableId());
        }
    }

    /**
     * Remove block state tracking when an SSTable is obsoleted/deleted.
     */
    public HMASSTableBlockState removeSSTableBlockState(long sstableId)
    {
        HMASSTableBlockState removed = blockStates.remove(sstableId);
        if (removed != null)
            removed.markObsolete();
        return removed;
    }

    public HMASSTableBlockState getBlockState(long sstableId)
    {
        return blockStates.get(sstableId);
    }

    public Collection<HMASSTableBlockState> getAllBlockStates()
    {
        return blockStates.values();
    }

    public int getDefragLwmPct()
    {
        return defragLwmPct;
    }

    public void setDefragLwmPct(int lwmPct)
    {
        this.defragLwmPct = Math.max(1, Math.min(99, lwmPct));
        for (HMASSTableBlockState state : blockStates.values())
        {
            maybeEnqueueForDefrag(state);
        }
    }

    public int getDefragQueueSize()
    {
        return defragQueue.size();
    }

    public Long pollDefragCandidate()
    {
        return defragQueue.poll();
    }

    /**
     * Returns all sealed SSTable IDs whose live byte utilization is currently below {@code defragLwmPct}.
     */
    public List<Long> getDefragCandidateIds()
    {
        List<Long> candidates = new ArrayList<>();
        for (HMASSTableBlockState state : blockStates.values())
        {
            if (state.needsDefrag(defragLwmPct))
                candidates.add(state.getSstableId());
        }
        return candidates;
    }

    public void recordDirectFree(long reclaimedBytes)
    {
        directFreedSSTableCount.increment();
        defraggedSSTableCount.increment();
        if (reclaimedBytes > 0)
            defragReclaimedByteCount.add(reclaimedBytes);
    }

    public void recordDefragStats(int defraggedSSTables, long movedRecords, long skippedRecords, long reclaimedBytes)
    {
        defragRunCount.increment();
        if (defraggedSSTables > 0)
            defraggedSSTableCount.add(defraggedSSTables);
        if (movedRecords > 0)
            defragMovedRecordCount.add(movedRecords);
        if (skippedRecords > 0)
            defragSkippedRecordCount.add(skippedRecords);
        if (reclaimedBytes > 0)
            defragReclaimedByteCount.add(reclaimedBytes);
    }

    public long getDirectFreedSSTableCount()
    {
        return directFreedSSTableCount.sum();
    }

    public long getDefragRunCount()
    {
        return defragRunCount.sum();
    }

    public long getDefraggedSSTableCount()
    {
        return defraggedSSTableCount.sum();
    }

    public long getDefragMovedRecordCount()
    {
        return defragMovedRecordCount.sum();
    }

    public long getDefragSkippedRecordCount()
    {
        return defragSkippedRecordCount.sum();
    }

    public long getDefragReclaimedByteCount()
    {
        return defragReclaimedByteCount.sum();
    }

    public long getTotalTrackedDiskBytes()
    {
        long total = 0;
        for (HMASSTableBlockState state : blockStates.values())
            if (!state.isObsolete())
                total += state.getWrittenBytes();
        return total;
    }

    public long getTotalTrackedLiveBytes()
    {
        long total = 0;
        for (HMASSTableBlockState state : blockStates.values())
            if (!state.isObsolete())
                total += state.getInUseBytes();
        return total;
    }

    private static final class LookupHint
    {
        HMAPrimaryIndex index;
        DecoratedKey key;
        HMARecordPointer pointer;
    }

    private static final ThreadLocal<LookupHint> threadLocalLookupHint = ThreadLocal.withInitial(LookupHint::new);

    public void setThreadLocalLookupHint(DecoratedKey key, HMARecordPointer pointer)
    {
        LookupHint hint = threadLocalLookupHint.get();
        hint.index = this;
        hint.key = key;
        hint.pointer = pointer;
    }

    public static void clearThreadLocalLookupHint()
    {
        LookupHint hint = threadLocalLookupHint.get();
        hint.index = null;
        hint.key = null;
        hint.pointer = null;
    }

    /**
     * Point lookup for a partition key.
     *
     * @param key Partition key to resolve
     * @param outPointer Output struct populated with exact SSTable file ID, byte offset and data length
     * @return true if key exists in index and is not deleted/tombstone, false otherwise
     */
    public boolean lookup(DecoratedKey key, HMARecordPointer outPointer)
    {
        LookupHint hint = threadLocalLookupHint.get();
        if (hint.index == this && hint.key == key && hint.pointer != null)
        {
            if (outPointer != hint.pointer)
                outPointer.copyFrom(hint.pointer);
            return true;
        }

        lookupCount.increment();
        long[] hash = threadLocalHashBuffer.get();
        HMARecordPointer.computeKeyHash(key, hash);
        int shardIdx = selectShard(hash[0], hash[1]);
        boolean found = shards[shardIdx].get(hash[0], hash[1], outPointer);
        if (found)
            hitCount.increment();
        else
            missCount.increment();
        return found;
    }

    /**
     * Record a direct SSTable Data.db seek that bypassed Bloom Filter, Key Cache, Index Summary, and Partition Index.
     */
    public void recordDirectReadBypass()
    {
        directReadBypassCount.increment();
    }

    public long getLookupCount()
    {
        return lookupCount.sum();
    }

    public long getHitCount()
    {
        return hitCount.sum();
    }

    public long getMissCount()
    {
        return missCount.sum();
    }

    public long getDirectReadBypassCount()
    {
        return directReadBypassCount.sum();
    }

    /**
     * Mark a key as deleted (tombstone) with timestamp for conflict resolution.
     */
    public boolean delete(DecoratedKey key, long timestamp)
    {
        long[] hash = threadLocalHashBuffer.get();
        HMARecordPointer.computeKeyHash(key, hash);
        int shardIdx = selectShard(hash[0], hash[1]);
        return shards[shardIdx].delete(hash[0], hash[1], timestamp, this);
    }

    /**
     * Total number of indexed entries across all shards.
     */
    public long totalEntries()
    {
        long total = 0;
        for (HMAPrimaryIndexShard shard : shards)
            total += shard.size();
        return total;
    }

    /**
     * Total native off-heap memory allocated in bytes.
     */
    public long offHeapMemoryAllocated()
    {
        long total = 0;
        for (HMAPrimaryIndexShard shard : shards)
            total += shard.allocatedBytes();
        return total;
    }

    @Override
    public void close()
    {
        blockStates.clear();
        defragQueue.clear();
        for (HMAPrimaryIndexShard shard : shards)
        {
            shard.close();
        }
    }
}
