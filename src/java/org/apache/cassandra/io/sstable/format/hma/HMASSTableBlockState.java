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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks live byte utilization ({@code inuse_sz}) per SSTable and per fixed-size Write Block (default 1 MiB),
 * modeled directly after Aerospike's {@code ssd_wblock_state} in {@code drv_ssd.c}.
 *
 * <p>As records are written into an SSTable, {@link #onRecordWritten(long, int, boolean)} increments both
 * {@code writtenBytes} and (if live) {@code inUseBytes} for the SSTable and the corresponding 1 MiB block.
 * When a subsequent write or delete in {@link HMAPrimaryIndex} supersedes an older record in this SSTable,
 * {@link #onRecordSuperseded(long, int)} atomically decrements {@code inUseBytes} ({@code inuse_sz}) by the
 * old record's byte size.
 *
 * <h3>Two-Tier Space Reclamation</h3>
 * <ul>
 *   <li><b>Tier 1 — Direct-Free ({@code inuse_sz == 0}):</b> When all records in a sealed SSTable have been
 *       overwritten or deleted ({@link #isCompletelyDead()}), the SSTable is reclaimed immediately via
 *       {@code LifecycleTransaction.obsolete} with <b>0 disk reads and 0 disk writes (Write Amplification = 0.0x)</b>.</li>
 *   <li><b>Tier 2 — Low-Water Mark Block Defragmentation ({@code 0 < inuse_pct < defrag_lwm_pct}):</b> When live
 *       utilization drops below the configured LWM threshold (default 50%), {@link org.apache.cassandra.db.compaction.HMABlockDefragTask}
 *       scans only the fragmented SSTable, skips dead records in O(1) via {@link HMAPrimaryIndex}, copies only
 *       still-live records forward, and frees the fragmented SSTable without N-way merge-sort compaction.</li>
 * </ul>
 */
public final class HMASSTableBlockState
{
    public static final int DEFAULT_BLOCK_SIZE_BYTES = 1024 * 1024; // 1 MiB write block

    private final long sstableId;
    private final int blockSizeBytes;

    private final AtomicLong totalWrittenBytes = new AtomicLong(0L);
    private final AtomicLong totalInUseBytes = new AtomicLong(0L);
    private final AtomicLong totalRecords = new AtomicLong(0L);
    private final AtomicLong liveRecords = new AtomicLong(0L);

    // Per 1-MiB logical block tracking (blockIdx -> writtenBytes / inUseBytes)
    private final ConcurrentHashMap<Integer, AtomicLong> blockWrittenBytes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AtomicLong> blockInUseBytes = new ConcurrentHashMap<>();

    private volatile boolean sealed = false;
    private final AtomicBoolean enqueuedForDefrag = new AtomicBoolean(false);
    private final AtomicBoolean obsolete = new AtomicBoolean(false);

    public HMASSTableBlockState(long sstableId)
    {
        this(sstableId, DEFAULT_BLOCK_SIZE_BYTES);
    }

    public HMASSTableBlockState(long sstableId, int blockSizeBytes)
    {
        this.sstableId = sstableId;
        this.blockSizeBytes = Math.max(4096, blockSizeBytes);
    }

    public long getSstableId()
    {
        return sstableId;
    }

    public int getBlockSizeBytes()
    {
        return blockSizeBytes;
    }

    public int blockIndexForOffset(long dataOffset)
    {
        if (dataOffset <= 0L)
            return 0;
        return (int) Math.min(Integer.MAX_VALUE, dataOffset / blockSizeBytes);
    }

    /**
     * Record that a partition of {@code dataLength} bytes at {@code dataOffset} was written to this SSTable.
     *
     * @param dataOffset byte offset in Data.db
     * @param dataLength serialized byte length of the partition
     * @param isLive     true if the record won LWW in HMAPrimaryIndex; false if it was already superseded on arrival
     */
    public void onRecordWritten(long dataOffset, int dataLength, boolean isLive)
    {
        int effectiveLen = Math.max(1, dataLength);
        int blockIdx = blockIndexForOffset(dataOffset);

        totalRecords.incrementAndGet();
        totalWrittenBytes.addAndGet(effectiveLen);
        blockWrittenBytes.computeIfAbsent(blockIdx, k -> new AtomicLong(0L)).addAndGet(effectiveLen);

        if (isLive)
        {
            liveRecords.incrementAndGet();
            totalInUseBytes.addAndGet(effectiveLen);
            blockInUseBytes.computeIfAbsent(blockIdx, k -> new AtomicLong(0L)).addAndGet(effectiveLen);
        }
        else
        {
            blockInUseBytes.computeIfAbsent(blockIdx, k -> new AtomicLong(0L));
        }
    }

    /**
     * Atomically decrement {@code inuse_sz} when an existing live record at {@code oldDataOffset}
     * with {@code oldDataLength} in this SSTable is overwritten or deleted in {@link HMAPrimaryIndex}.
     *
     * @return the resulting {@code inUseBytes} for this SSTable after decrementing
     */
    public long onRecordSuperseded(long oldDataOffset, int oldDataLength)
    {
        int effectiveLen = Math.max(1, oldDataLength);
        int blockIdx = blockIndexForOffset(oldDataOffset);

        liveRecords.updateAndGet(v -> Math.max(0L, v - 1L));

        AtomicLong blockInUse = blockInUseBytes.get(blockIdx);
        if (blockInUse != null)
        {
            blockInUse.updateAndGet(v -> Math.max(0L, v - effectiveLen));
        }

        return totalInUseBytes.updateAndGet(v -> Math.max(0L, v - effectiveLen));
    }

    public void markSealed()
    {
        this.sealed = true;
    }

    public boolean isSealed()
    {
        return sealed;
    }

    public boolean markEnqueuedForDefrag()
    {
        return enqueuedForDefrag.compareAndSet(false, true);
    }

    public void clearEnqueuedForDefrag()
    {
        enqueuedForDefrag.set(false);
    }

    public boolean isEnqueuedForDefrag()
    {
        return enqueuedForDefrag.get();
    }

    public boolean markObsolete()
    {
        return obsolete.compareAndSet(false, true);
    }

    public boolean isObsolete()
    {
        return obsolete.get();
    }

    public long getWrittenBytes()
    {
        return totalWrittenBytes.get();
    }

    public long getInUseBytes()
    {
        return totalInUseBytes.get();
    }

    public long getDeadBytes()
    {
        return Math.max(0L, totalWrittenBytes.get() - totalInUseBytes.get());
    }

    public long getTotalRecords()
    {
        return totalRecords.get();
    }

    public long getLiveRecords()
    {
        return liveRecords.get();
    }

    public long getDeadRecords()
    {
        return Math.max(0L, totalRecords.get() - liveRecords.get());
    }

    /**
     * Live byte utilization percentage (0 to 100), analogous to Aerospike's {@code (inuse_sz * 100) / wblock_size}.
     */
    public int getLiveUtilizationPct()
    {
        long written = totalWrittenBytes.get();
        if (written <= 0L)
            return 100;
        long inUse = totalInUseBytes.get();
        if (inUse <= 0L && liveRecords.get() <= 0L)
            return 0;
        return (int) Math.min(100L, (inUse * 100L) / written);
    }

    /**
     * @return true if this SSTable is sealed and has 0 live bytes and 0 live records (eligible for 0-I/O Direct-Free).
     */
    public boolean isCompletelyDead()
    {
        return sealed
               && !obsolete.get()
               && totalRecords.get() > 0L
               && liveRecords.get() == 0L
               && totalInUseBytes.get() == 0L;
    }

    /**
     * @return true if this SSTable is sealed and its live utilization is strictly below {@code defragLwmPct}
     *         (or it is completely dead).
     */
    public boolean needsDefrag(int defragLwmPct)
    {
        if (!sealed || obsolete.get() || totalRecords.get() <= 0L)
            return false;
        if (isCompletelyDead())
            return true;
        if (getLiveUtilizationPct() < defragLwmPct)
            return true;
        int totalBlocks = getBlockCount();
        if (totalBlocks > 1)
        {
            int underLwmBlocks = getZeroUseBlockCount() + getFragmentedBlockCount(defragLwmPct);
            return underLwmBlocks * 2 >= totalBlocks;
        }
        return false;
    }

    /**
     * Total number of fixed-size write blocks tracked in this SSTable.
     */
    public int getBlockCount()
    {
        return blockWrittenBytes.size();
    }

    /**
     * Number of fixed-size write blocks in this SSTable that have {@code inuse_sz == 0} (100% dead block).
     */
    public int getZeroUseBlockCount()
    {
        int count = 0;
        for (Map.Entry<Integer, AtomicLong> entry : blockWrittenBytes.entrySet())
        {
            if (entry.getValue().get() > 0L)
            {
                AtomicLong inUse = blockInUseBytes.get(entry.getKey());
                if (inUse == null || inUse.get() <= 0L)
                    count++;
            }
        }
        return count;
    }

    /**
     * Number of fixed-size write blocks in this SSTable whose live utilization is {@code 0 < pct < defragLwmPct}.
     */
    public int getFragmentedBlockCount(int defragLwmPct)
    {
        int count = 0;
        for (Map.Entry<Integer, AtomicLong> entry : blockWrittenBytes.entrySet())
        {
            long written = entry.getValue().get();
            if (written > 0L)
            {
                AtomicLong inUseRef = blockInUseBytes.get(entry.getKey());
                long inUse = inUseRef != null ? inUseRef.get() : 0L;
                if (inUse > 0L)
                {
                    int pct = (int) ((inUse * 100L) / written);
                    if (pct < defragLwmPct)
                        count++;
                }
            }
        }
        return count;
    }

    @Override
    public String toString()
    {
        return String.format("HMASSTableBlockState{sstableId=%d, sealed=%s, written=%dB, inUse=%dB (%d%%), liveRecords=%d/%d, blocks=%d (zero=%d)}",
                             sstableId,
                             sealed,
                             totalWrittenBytes.get(),
                             totalInUseBytes.get(),
                             getLiveUtilizationPct(),
                             liveRecords.get(),
                             totalRecords.get(),
                             getBlockCount(),
                             getZeroUseBlockCount());
    }
}
