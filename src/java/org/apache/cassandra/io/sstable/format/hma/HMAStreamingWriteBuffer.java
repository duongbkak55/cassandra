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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;

/**
 * Aerospike-inspired fixed-size Streaming Write Buffer (SWB) for Cassandra (Parts 4 &amp; 5).
 *
 * <p>Eliminates Cassandra's traditional Double-Write penalty (CommitLog append + Memtable flush)
 * and JVM GC pressure by cloning incoming {@link PartitionUpdate} keys and cells directly into a
 * recycled 64-byte cache-line aligned {@link HMAOffHeapArena} slab from {@link HMAOffHeapArenaPool}.
 *
 * <p>Mutations are indexed in an $O(1)$ lock-free {@link ConcurrentHashMap} during ingestion and
 * sorted once when the sealed block is flushed to the SSTable data file via Direct I/O.
 */
public final class HMAStreamingWriteBuffer
{
    private static final EnsureOnHeap ON_HEAP_CLONER = new EnsureOnHeap.CloneToHeap();

    private final long bufferId;
    private final TableMetadata metadata;
    private final int capacityBytes;
    private final HMAOffHeapArena arena;
    private final ConcurrentHashMap<DecoratedKey, PartitionUpdate> partitions = new ConcurrentHashMap<>(2048);
    private final AtomicLong bufferedBytes = new AtomicLong(0);
    private final AtomicInteger mutationCount = new AtomicInteger(0);
    private final AtomicBoolean sealed = new AtomicBoolean(false);
    private final AtomicBoolean released = new AtomicBoolean(false);

    public HMAStreamingWriteBuffer(long bufferId, TableMetadata metadata, int capacityBytes)
    {
        this.bufferId = bufferId;
        this.metadata = metadata;
        this.capacityBytes = Math.max(4096, capacityBytes);
        this.arena = HMAOffHeapArenaPool.instance().newArena(this.capacityBytes);
    }

    /**
     * Appends a {@link PartitionUpdate} to this Streaming Write Buffer, cloning keys and cells
     * into the off-heap {@link HMAOffHeapArena}.
     *
     * @return {@code true} if the buffer was open and accepted the update, {@code false} if already sealed
     */
    public boolean append(PartitionUpdate update)
    {
        if (sealed.get())
            return false;

        DecoratedKey key = update.partitionKey();
        int estimatedSize = Math.max(64, update.dataSize() + key.getKeyLength() + 16);
        Cloner cloner = arena.cloner();

        partitions.compute(key, (k, existing) -> {
            DecoratedKey targetKey = existing != null ? existing.partitionKey() : cloner.clone(k);
            BTreePartitionData currentData = existing != null ? existing.holder() : null;
            ArenaPartitionUpdater updater = new ArenaPartitionUpdater(cloner);
            BTreePartitionData mergedData = updater.mergePartitions(currentData, update);
            MutableDeletionInfo delInfo = mergedData.deletionInfo instanceof MutableDeletionInfo
                                          ? (MutableDeletionInfo) mergedData.deletionInfo
                                          : mergedData.deletionInfo.mutableCopy();
            return PartitionUpdate.unsafeConstruct(metadata, targetKey, mergedData, delInfo, false);
        });

        bufferedBytes.addAndGet(estimatedSize);
        mutationCount.incrementAndGet();
        return true;
    }

    /**
     * Returns {@code true} if the accumulated payload has reached or exceeded the streaming block capacity.
     */
    public boolean isFull()
    {
        return bufferedBytes.get() >= capacityBytes;
    }

    public boolean isEmpty()
    {
        return partitions.isEmpty();
    }

    public boolean seal()
    {
        return sealed.compareAndSet(false, true);
    }

    public boolean isSealed()
    {
        return sealed.get();
    }

    public long getBufferId()
    {
        return bufferId;
    }

    public TableMetadata getMetadata()
    {
        return metadata;
    }

    public int getCapacityBytes()
    {
        return capacityBytes;
    }

    public long getBufferedBytes()
    {
        return bufferedBytes.get();
    }

    public long getArenaAllocatedBytes()
    {
        return arena.allocatedBytes();
    }

    public int getMutationCount()
    {
        return mutationCount.get();
    }

    public int getPartitionCount()
    {
        return partitions.size();
    }

    public Collection<PartitionUpdate> getSortedPartitions()
    {
        List<PartitionUpdate> sorted = new ArrayList<>(partitions.values());
        sorted.sort((a, b) -> a.partitionKey().compareTo(b.partitionKey()));
        return sorted;
    }

    public PartitionUpdate getPartition(DecoratedKey key)
    {
        return partitions.get(key);
    }

    /**
     * Queries a single partition from this in-flight Streaming Write Buffer before it is flushed to disk.
     * Clones returned cells onto the heap so concurrent buffer release after flush cannot invalidate read cells.
     */
    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             ClusteringIndexFilter filter,
                                             ColumnFilter columnFilter)
    {
        PartitionUpdate update = partitions.get(key);
        if (update == null || update.isEmpty())
            return null;
        return ON_HEAP_CLONER.applyToPartition(filter.getUnfilteredRowIterator(columnFilter, update));
    }

    /**
     * Releases the {@link HMAOffHeapArena} slabs back to {@link HMAOffHeapArenaPool} once the block has been flushed.
     */
    public void release()
    {
        if (released.compareAndSet(false, true))
        {
            partitions.clear();
            arena.close();
        }
    }

    private static final class ArenaPartitionUpdater extends BTreePartitionUpdater
    {
        ArenaPartitionUpdater(Cloner cloner)
        {
            super(null, cloner, null, UpdateTransaction.NO_OP);
        }

        @Override
        public void reportAllocatedMemory()
        {
            if (contextCloner != null && contextCloner != cloner)
                contextCloner.adjustUnused();
        }
    }
}
