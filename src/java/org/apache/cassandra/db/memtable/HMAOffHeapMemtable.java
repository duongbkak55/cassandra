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

package org.apache.cassandra.db.memtable;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.github.jamm.Unmetered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.hma.HMAOffHeapArena;
import org.apache.cassandra.io.sstable.format.hma.HMAOffHeapArenaPool;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Aerospike-inspired Pluggable Off-Heap Arena Memtable (Part 5).
 *
 * <p>Stores all partition keys ({@link org.apache.cassandra.db.NativeDecoratedKey}), clusterings
 * ({@link org.apache.cassandra.db.NativeClustering}), and cells ({@link org.apache.cassandra.db.rows.NativeCell})
 * inside fixed-size 1 MiB off-heap slabs managed by {@link HMAOffHeapArena} and {@link HMAOffHeapArenaPool}.
 *
 * <p>Key architectural advantages:
 * <ul>
 *   <li><b>Zero-GC Off-Heap Storage</b>: Key and cell payloads live in 64-byte cache-line aligned native slabs,
 *       invisible to the JVM garbage collector.</li>
 *   <li><b>O(1) Point Lookup &amp; Point Mutation</b>: Maintains a lock-free {@link ConcurrentHashMap} keyed by
 *       {@link org.apache.cassandra.db.NativeDecoratedKey} alongside the token-sorted skip-list so point reads
 *       and updates bypass $O(\log N)$ tree traversal.</li>
 *   <li><b>Zero-Copy Off-Heap Flush</b>: During memtable flush ({@link #getFlushSet}), cells are streamed directly
 *       from native off-heap memory to the SSTable writer via {@code NativeAccessor.write} without allocating
 *       intermediate heap {@code ByteBuffer} or {@code BufferCell} objects.</li>
 *   <li><b>O(1) Slab Recycling</b>: On {@link #discard()}, all standard 1 MiB slabs are returned directly to
 *       {@link HMAOffHeapArenaPool} for immediate reuse without {@code free()}/{@code malloc()} churn.</li>
 * </ul>
 */
public class HMAOffHeapMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(HMAOffHeapMemtable.class);

    public static final Factory FACTORY = HMAOffHeapMemtableFactory.INSTANCE;

    private static final EnsureOnHeap ON_HEAP_CLONER = new EnsureOnHeap.CloneToHeap();
    private static final int PARTITION_HEAP_OVERHEAD = 96;
    private static final int OFFHEAP_QUOTA_DIVISOR = Math.max(1, Integer.getInteger("cassandra.hma.memtable.offheap_divisor", 2));
    private static final long MAX_OFFHEAP_MEMTABLE_BYTES = Math.max(32L, Long.getLong("cassandra.hma.memtable.max_offheap_mb", 256L)) * 1024L * 1024L;

    private static final AtomicLong DIRECT_LOOKUP_HITS = new AtomicLong(0);
    private static final AtomicLong DIRECT_LOOKUP_MISSES = new AtomicLong(0);
    private static final AtomicLong OFFHEAP_PUT_OPS = new AtomicLong(0);

    @Unmetered
    private final HMAOffHeapArena arena;

    @Unmetered
    private final MemtableAllocator arenaPartitionAllocator;

    /**
     * O(1) hash index for point lookups and point updates using zero-allocation NativeDecoratedKey hashCode/equals.
     */
    private final ConcurrentHashMap<DecoratedKey, AtomicBTreePartition> keyIndex = new ConcurrentHashMap<>(4096);

    /**
     * Token-sorted navigable map for range queries and zero-copy sorted SSTable flushes.
     */
    private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();

    private final AtomicLong liveDataSize = new AtomicLong(0);
    private final AtomicBoolean arenaClosed = new AtomicBoolean(false);
    private final AtomicBoolean offHeapCeilingFlushScheduled = new AtomicBoolean(false);

    public HMAOffHeapMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                              TableMetadataRef metadataRef,
                              Owner owner)
    {
        this(commitLogLowerBound, metadataRef, owner, HMAOffHeapArenaPool.instance().slabSizeBytes());
    }

    public HMAOffHeapMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                              TableMetadataRef metadataRef,
                              Owner owner,
                              int slabSizeBytes)
    {
        super(commitLogLowerBound, metadataRef, owner);
        this.arena = HMAOffHeapArenaPool.instance().newArena(slabSizeBytes);
        this.arenaPartitionAllocator = new ArenaMemtableAllocator(this.allocator, this.arena);
    }

    public static long getDirectLookupHits()
    {
        return DIRECT_LOOKUP_HITS.get();
    }

    public static long getDirectLookupMisses()
    {
        return DIRECT_LOOKUP_MISSES.get();
    }

    public static long getOffHeapPutOps()
    {
        return OFFHEAP_PUT_OPS.get();
    }

    @VisibleForTesting
    public HMAOffHeapArena getArena()
    {
        return arena;
    }

    @Override
    public boolean isClean()
    {
        return partitions.isEmpty();
    }

    @Override
    public Token lastToken()
    {
        Iterator<PartitionPosition> iterator = partitions.keySet().iterator();
        if (iterator.hasNext())
            return iterator.next().getToken();
        return null;
    }

    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, boolean assumeMissing)
    {
        long initialSize = 0;
        HMAOffHeapArena.HMAOffHeapCloner cloner = arena.trackingCloner(opGroup);
        DecoratedKey key = update.partitionKey();

        AtomicBTreePartition previous = assumeMissing ? null : keyIndex.get(key);
        if (previous == null)
        {
            DecoratedKey cloneKey = cloner.clone(key);
            AtomicBTreePartition empty = new AtomicBTreePartition(metadata, cloneKey, arenaPartitionAllocator);
            previous = keyIndex.putIfAbsent(cloneKey, empty);
            if (previous == null)
            {
                partitions.put(cloneKey, empty);
                previous = empty;
                allocator.onHeap().allocate(PARTITION_HEAP_OVERHEAD);
                initialSize = 8 + cloneKey.getKeyLength();
            }
        }

        BTreePartitionUpdater updater = previous.addAll(update, cloner, opGroup, indexer);
        long opArenaQuota = cloner.allocatedBytes() / OFFHEAP_QUOTA_DIVISOR;
        if (opArenaQuota > 0)
        {
            // Scale off-heap arena allocation against the on-heap threshold so off-heap memtables auto-flush predictably
            allocator.onHeap().allocate(opArenaQuota);
        }
        if (arena.allocatedBytes() >= MAX_OFFHEAP_MEMTABLE_BYTES
            && offHeapCeilingFlushScheduled.compareAndSet(false, true))
        {
            owner.signalFlushRequired(this, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
        }

        updateMin(minTimestamp, update.stats().minTimestamp);
        updateMin(minLocalDeletionTime, update.stats().minLocalDeletionTime);
        liveDataSize.addAndGet(initialSize + updater.dataSize);
        columnsCollector.update(update.columns());
        statsCollector.update(update.stats());
        currentOperations.addAndGet(update.operationCount());
        OFFHEAP_PUT_OPS.incrementAndGet();
        return updater.colUpdateTimeDelta;
    }

    @Override
    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason, TableMetadata latest)
    {
        if (reason == ColumnFamilyStore.FlushReason.SCHEMA_CHANGE)
        {
            return initialComparator != latest.comparator
                   || HMAOffHeapMemtableFactory.resolveFactory(latest) != HMAOffHeapMemtableFactory.INSTANCE;
        }
        return super.shouldSwitch(reason, latest);
    }

    @Override
    public long partitionCount()
    {
        return keyIndex.size();
    }

    @Override
    public long getLiveDataSize()
    {
        return liveDataSize.get();
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage stats)
    {
        super.addMemoryUsageTo(stats);
        stats.ownsOffHeap += arena.allocatedBytes();
    }

    @Override
    public void discard()
    {
        try
        {
            if (arenaClosed.compareAndSet(false, true))
                arena.close();
        }
        finally
        {
            super.discard();
        }
    }

    @Override
    public String toString()
    {
        MemoryUsage usage = Memtable.getMemoryUsage(this);
        return String.format("HMAOffHeapMemtable-%s@%s(%s serialized bytes, %s arena off-heap, %s ops, %s)",
                             metadata.get().name,
                             hashCode(),
                             FBUtilities.prettyPrintMemory(getLiveDataSize()),
                             FBUtilities.prettyPrintMemory(arena.allocatedBytes()),
                             operationCount(),
                             usage);
    }

    Partition getPartition(DecoratedKey key)
    {
        AtomicBTreePartition p = keyIndex.get(key);
        if (p != null)
        {
            DIRECT_LOOKUP_HITS.incrementAndGet();
            return p;
        }
        DIRECT_LOOKUP_MISSES.incrementAndGet();
        return null;
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter selectedColumns,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        return ON_HEAP_CLONER.applyToPartition(p.unfilteredIterator(selectedColumns, slices, reversed));
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        return ON_HEAP_CLONER.applyToPartition(p.unfilteredIterator());
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                         final DataRange dataRange,
                                                         SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeLeft = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeRight = isBound || keyRange instanceof Range;
        Map<PartitionPosition, AtomicBTreePartition> subMap = getPartitionsSubMap(left,
                                                                                  includeLeft,
                                                                                  right,
                                                                                  includeRight);

        return new OffHeapMemtableUnfilteredPartitionIterator(metadata.get(), subMap, columnFilter, dataRange);
    }

    private Map<PartitionPosition, AtomicBTreePartition> getPartitionsSubMap(PartitionPosition left,
                                                                             boolean includeLeft,
                                                                             PartitionPosition right,
                                                                             boolean includeRight)
    {
        if (left != null && left.isMinimum())
            left = null;
        if (right != null && right.isMinimum())
            right = null;

        try
        {
            if (left == null)
                return right == null ? partitions : partitions.headMap(right, includeRight);
            else
                return right == null
                       ? partitions.tailMap(left, includeLeft)
                       : partitions.subMap(left, includeLeft, right, includeRight);
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Invalid range requested {} - {}", left, right);
            throw e;
        }
    }

    @Override
    public FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Map<PartitionPosition, AtomicBTreePartition> toFlush = getPartitionsSubMap(from, true, to, false);
        long keysSize = 0;
        long keyCount = 0;
        TableMetadata currentTableMetadata = metadata();

        for (PartitionPosition key : toFlush.keySet())
        {
            assert key instanceof DecoratedKey;
            keysSize += ((DecoratedKey) key).getKeyLength();
            ++keyCount;
        }

        final long partitionKeysSize = keysSize;
        final long partitionCount = keyCount;

        return new AbstractFlushablePartitionSet<AtomicBTreePartition>()
        {
            private final TableMetadata tableMetadata = currentTableMetadata;

            @Override
            public Memtable memtable()
            {
                return HMAOffHeapMemtable.this;
            }

            @Override
            public PartitionPosition from()
            {
                return from;
            }

            @Override
            public PartitionPosition to()
            {
                return to;
            }

            @Override
            public long partitionCount()
            {
                return partitionCount;
            }

            @Override
            public Iterator<AtomicBTreePartition> iterator()
            {
                return toFlush.values().iterator();
            }

            @Override
            public long partitionKeysSize()
            {
                return partitionKeysSize;
            }

            @Override
            public TableMetadata metadata()
            {
                return tableMetadata;
            }
        };
    }

    private static final class OffHeapMemtableUnfilteredPartitionIterator
        extends AbstractUnfilteredPartitionIterator
        implements UnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<Map.Entry<PartitionPosition, AtomicBTreePartition>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        OffHeapMemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   Map<PartitionPosition, AtomicBTreePartition> map,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.metadata = metadata;
            this.iter = map.entrySet().iterator();
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            Map.Entry<PartitionPosition, AtomicBTreePartition> entry = iter.next();
            assert entry.getKey() instanceof DecoratedKey;
            DecoratedKey key = (DecoratedKey) entry.getKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);
            return ON_HEAP_CLONER.applyToPartition(filter.getUnfilteredRowIterator(columnFilter, entry.getValue()));
        }
    }

    /**
     * MemtableAllocator adapter that delegates memory accounting to the parent allocator while using
     * {@link EnsureOnHeap#NOOP} on the partition iterator so {@link Flushing} streams native off-heap cells
     * directly to SSTables without heap copies.
     */
    private static final class ArenaMemtableAllocator extends MemtableAllocator
    {
        private final HMAOffHeapArena arena;

        ArenaMemtableAllocator(MemtableAllocator delegate, HMAOffHeapArena arena)
        {
            super(delegate.onHeap(), delegate.offHeap());
            this.arena = arena;
        }

        @Override
        public EnsureOnHeap ensureOnHeap()
        {
            return EnsureOnHeap.NOOP;
        }

        @Override
        public Cloner cloner(OpOrder.Group opGroup)
        {
            return arena.cloner(opGroup);
        }
    }
}
