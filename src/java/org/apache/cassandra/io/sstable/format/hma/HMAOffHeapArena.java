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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.NativeClustering;
import org.apache.cassandra.db.NativeDecoratedKey;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.NativeCell;
import org.apache.cassandra.io.sstable.format.hma.HMAOffHeapArenaPool.Slab;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AddressBasedAllocator;
import org.apache.cassandra.utils.memory.Cloner;

/**
 * Thread-striped lock-free Off-Heap Arena backed by fixed-size slabs from {@link HMAOffHeapArenaPool} (Part 5).
 *
 * <p>Unlike Cassandra's default {@code NativeAllocator} (where all writer threads contend on a single
 * {@code AtomicReference<Region> currentRegion} and call {@code free()} on every region upon discard),
 * {@link HMAOffHeapArena}:
 * <ul>
 *   <li>Stripes active slabs across {@link #STRIPE_COUNT} slots indexed by thread ID to eliminate CAS contention.</li>
 *   <li>Supports context-aware chunk pre-allocation ({@link HMAOffHeapCloner}) so all cells and clusterings of a
 *       partition update are bump-allocated in a single thread-local slice without atomic instructions.</li>
 *   <li>Recycles all standard slabs back to {@link HMAOffHeapArenaPool} in O(1) upon {@link #close()}.</li>
 * </ul>
 */
public final class HMAOffHeapArena implements AddressBasedAllocator, AutoCloseable
{
    private static final int STRIPE_COUNT = 32;
    private static final int STRIPE_MASK = STRIPE_COUNT - 1;

    private final HMAOffHeapArenaPool pool;
    private final int slabSizeBytes;
    private final int maxPooledAllocationSize;
    private final AtomicReferenceArray<Slab> activeStripes = new AtomicReferenceArray<>(STRIPE_COUNT);
    private final ConcurrentLinkedQueue<Slab> ownedSlabs = new ConcurrentLinkedQueue<>();
    private final AtomicLong allocatedBytes = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    HMAOffHeapArena(HMAOffHeapArenaPool pool, int slabSizeBytes)
    {
        this.pool = pool;
        this.slabSizeBytes = Math.max(64 * 1024, slabSizeBytes);
        this.maxPooledAllocationSize = this.slabSizeBytes >>> 2; // Up to 256 KiB from shared stripe slab
    }

    public long allocate(int sizeToAllocate)
    {
        return allocate(sizeToAllocate, null);
    }

    @Override
    public long allocate(int sizeToAllocate, OpOrder.Group opGroup)
    {
        int size = Math.max(8, (sizeToAllocate + 7) & ~7);
        allocatedBytes.addAndGet(size);
        pool.onBytesAllocated(size);

        if (size > maxPooledAllocationSize)
        {
            Slab oversize = pool.acquireSlab(size);
            ownedSlabs.add(oversize);
            long addr = oversize.allocate(size);
            if (addr < 0)
                throw new IllegalStateException("Oversize slab failed to satisfy allocation of " + size + " bytes");
            return addr;
        }

        int stripe = ((int) Thread.currentThread().getId()) & STRIPE_MASK;
        while (true)
        {
            Slab current = activeStripes.get(stripe);
            if (current != null)
            {
                long addr = current.allocate(size);
                if (addr >= 0)
                    return addr;
            }

            Slab next = pool.acquireSlab(slabSizeBytes);
            if (activeStripes.compareAndSet(stripe, current, next))
            {
                ownedSlabs.add(next);
                long addr = next.allocate(size);
                if (addr >= 0)
                    return addr;
            }
            else
            {
                pool.releaseSlab(next);
            }
        }
    }

    public DecoratedKey cloneKey(DecoratedKey key)
    {
        return cloneKey(key, null);
    }

    public DecoratedKey cloneKey(DecoratedKey key, OpOrder.Group writeOp)
    {
        if (key instanceof NativeDecoratedKey)
            return key;
        return new NativeDecoratedKey(key.getToken(), this, writeOp, key.getKey());
    }

    public Cloner cloner()
    {
        return new HMAOffHeapCloner(this, null, 0, new long[1]);
    }

    public Cloner cloner(OpOrder.Group opGroup)
    {
        return new HMAOffHeapCloner(this, opGroup, 0, new long[1]);
    }

    public HMAOffHeapCloner trackingCloner(OpOrder.Group opGroup)
    {
        return new HMAOffHeapCloner(this, opGroup, 0, new long[1]);
    }

    public long getAllocatedBytes()
    {
        return allocatedBytes.get();
    }

    public long allocatedBytes()
    {
        return allocatedBytes.get();
    }

    public int getOwnedSlabCount()
    {
        return ownedSlabs.size();
    }

    public int ownedSlabCount()
    {
        return ownedSlabs.size();
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public void close()
    {
        if (closed.compareAndSet(false, true))
        {
            for (int i = 0; i < STRIPE_COUNT; i++)
                activeStripes.set(i, null);

            Slab slab;
            while ((slab = ownedSlabs.poll()) != null)
            {
                pool.releaseSlab(slab);
            }
            pool.onArenaClosed(allocatedBytes.get());
        }
    }

    /**
     * Context-aware off-heap cloner that pre-allocates a single contiguous slice from {@link HMAOffHeapArena}
     * for all cells and clusterings of a partition mutation, eliminating per-cell atomic operations.
     */
    public static final class HMAOffHeapCloner implements Cloner, AddressBasedAllocator
    {
        private final HMAOffHeapArena arena;
        private final OpOrder.Group opGroup;
        private final long chunkAddress;
        private final int chunkLimit;
        private final long[] threadAllocated;
        private int chunkOffset = 0;

        HMAOffHeapCloner(HMAOffHeapArena arena, OpOrder.Group opGroup, int preallocatedSize, long[] threadAllocated)
        {
            this.arena = arena;
            this.opGroup = opGroup;
            this.threadAllocated = threadAllocated;
            int aligned = preallocatedSize > 0 ? ((preallocatedSize + 7) & ~7) : 0;
            this.chunkLimit = aligned;
            if (aligned > 0)
            {
                this.chunkAddress = arena.allocate(aligned, opGroup);
                threadAllocated[0] += Math.max(8, aligned);
            }
            else
            {
                this.chunkAddress = -1L;
            }
        }

        public long allocatedBytes()
        {
            return threadAllocated[0];
        }

        @Override
        public DecoratedKey clone(DecoratedKey key)
        {
            if (!(key instanceof NativeDecoratedKey))
                threadAllocated[0] += Math.max(8, (key.getKeyLength() + 4 + 7) & ~7);
            return arena.cloneKey(key, opGroup);
        }

        @Override
        public Clustering<?> clone(Clustering<?> clustering)
        {
            if (clustering != Clustering.STATIC_CLUSTERING)
                return new NativeClustering(this, opGroup, clustering);
            return Clustering.STATIC_CLUSTERING;
        }

        @Override
        public int estimateCloneSize(Clustering<?> clustering)
        {
            return (NativeClustering.estimateAllocationSize(clustering) + 7) & ~7;
        }

        @Override
        public Cell<?> clone(Cell<?> cell)
        {
            return NativeCell.build(this, opGroup, cell);
        }

        @Override
        public int estimateCloneSize(Cell<?> cell)
        {
            return (NativeCell.estimateAllocationSize(cell) + 7) & ~7;
        }

        @Override
        public boolean isContextAwareCloningSupported()
        {
            return true;
        }

        @Override
        public Cloner createContextAwareCloner(int estimatedCloneSize)
        {
            return new HMAOffHeapCloner(arena, opGroup, estimatedCloneSize, threadAllocated);
        }

        @Override
        public long allocate(int sizeToAllocate, OpOrder.Group group)
        {
            int aligned = (sizeToAllocate + 7) & ~7;
            if (chunkAddress != -1L && chunkOffset + aligned <= chunkLimit)
            {
                long addr = chunkAddress + chunkOffset;
                chunkOffset += aligned;
                return addr;
            }
            threadAllocated[0] += Math.max(8, aligned);
            return arena.allocate(aligned, group);
        }

        @Override
        public void adjustUnused()
        {
            // Unused tail inside pre-allocated chunk remains inside the arena slab and is reclaimed on arena close.
        }
    }
}
