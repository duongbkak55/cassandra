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

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.memory.MemoryUtil;

/**
 * Aerospike-inspired (`cf_arenax` / jemalloc) Fixed-Size Off-Heap Slab Pool & Java 21+ FFM Arena bridge (Part 5).
 *
 * <p>Eliminates both JVM GC pauses and OS page-table thrashing (`malloc`/`free` overhead on every Memtable
 * or Streaming Write Buffer discard) by maintaining a lock-free pool of cache-line-aligned (64B) native memory
 * slabs (default 1 MiB per slab). Supports Java 21+ Foreign Function & Memory (`java.lang.foreign.Arena.ofShared()`)
 * when running on JDK 21+, while falling back cleanly to 64-byte aligned native slabs via {@link MemoryUtil#allocate(long)}
 * on JDK 17+.
 */
public final class HMAOffHeapArenaPool
{
    private static final Logger logger = LoggerFactory.getLogger(HMAOffHeapArenaPool.class);

    public static final String ENABLED_PROP = "cassandra.hma.arena.enabled";
    public static final String SLAB_SIZE_PROP = "cassandra.hma.arena.slab_size_bytes";
    public static final String MAX_POOLED_SLABS_PROP = "cassandra.hma.arena.max_pooled_slabs";
    public static final String USE_FFM_PROP = "cassandra.hma.arena.use_ffm";

    public static final int DEFAULT_SLAB_SIZE_BYTES = 1024 * 1024; // 1 MiB slab (matches Aerospike wblock / arena unit)
    public static final int DEFAULT_MAX_POOLED_SLABS = 256;        // Up to 256 MiB pooled off-heap slabs
    public static final int CACHE_LINE_ALIGNMENT = 64;

    private static final HMAOffHeapArenaPool INSTANCE = new HMAOffHeapArenaPool();

    private volatile boolean enabled = Boolean.parseBoolean(System.getProperty(ENABLED_PROP, "true"));

    // Optional Java 21+ FFM Arena reflection handles
    private static final boolean FFM_AVAILABLE;
    private static final Method FFM_ARENA_OF_SHARED;
    private static final Method FFM_ARENA_ALLOCATE;
    private static final Method FFM_ARENA_CLOSE;
    private static final Method FFM_SEGMENT_ADDRESS;

    static
    {
        boolean available = false;
        Method ofShared = null;
        Method allocate = null;
        Method close = null;
        Method address = null;
        if (Boolean.parseBoolean(System.getProperty(USE_FFM_PROP, "true")))
        {
            try
            {
                Class<?> arenaClass = Class.forName("java.lang.foreign.Arena");
                Class<?> segmentClass = Class.forName("java.lang.foreign.MemorySegment");
                ofShared = arenaClass.getMethod("ofShared");
                allocate = arenaClass.getMethod("allocate", long.class, long.class);
                close = arenaClass.getMethod("close");
                address = segmentClass.getMethod("address");
                available = true;
                logger.info("HMA Part 5: Detected Java 21+ Foreign Function & Memory (FFM) Arena API");
            }
            catch (Throwable ignored)
            {
                // Running on JDK 17-20 or without --enable-preview; uses 64B-aligned native slab allocator
            }
        }
        FFM_AVAILABLE = available;
        FFM_ARENA_OF_SHARED = ofShared;
        FFM_ARENA_ALLOCATE = allocate;
        FFM_ARENA_CLOSE = close;
        FFM_SEGMENT_ADDRESS = address;
    }

    /**
     * A 64-byte cache-line-aligned off-heap memory slab managed by {@link HMAOffHeapArenaPool}.
     */
    public static final class Slab
    {
        final long rawPeer;
        final long alignedAddress;
        final int capacity;
        final Object ffmArena;
        final boolean poolable;
        final AtomicInteger cursor = new AtomicInteger(0);

        Slab(long rawPeer, long alignedAddress, int capacity, Object ffmArena, boolean poolable)
        {
            this.rawPeer = rawPeer;
            this.alignedAddress = alignedAddress;
            this.capacity = capacity;
            this.ffmArena = ffmArena;
            this.poolable = poolable;
        }

        /**
         * Lock-free bump allocation of {@code size} bytes (rounded up to 8-byte alignment).
         *
         * @return native memory address, or {@code -1L} if this slab does not have enough remaining space
         */
        public long allocate(int size)
        {
            int alignedSize = (size + 7) & ~7;
            int offset = cursor.getAndAdd(alignedSize);
            if (offset + alignedSize > capacity)
                return -1L;
            return alignedAddress + offset;
        }

        public int usedBytes()
        {
            return Math.min(capacity, cursor.get());
        }

        void reset()
        {
            cursor.set(0);
        }

        void destroy()
        {
            if (ffmArena != null)
            {
                try
                {
                    FFM_ARENA_CLOSE.invoke(ffmArena);
                }
                catch (Throwable t)
                {
                    logger.warn("HMA: Failed to close FFM Arena", t);
                }
            }
            else if (rawPeer != 0L)
            {
                MemoryUtil.free(rawPeer);
            }
        }
    }

    private final ConcurrentLinkedQueue<Slab> freeSlabs = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pooledSlabsCount = new AtomicInteger(0);
    private final AtomicInteger activeSlabsCount = new AtomicInteger(0);
    private final AtomicLong activeArenaBytes = new AtomicLong(0);
    private final LongAdder totalBytesAllocated = new LongAdder();
    private final LongAdder slabHits = new LongAdder();
    private final LongAdder slabMisses = new LongAdder();
    private final LongAdder arenasCreated = new LongAdder();
    private final LongAdder arenasClosed = new LongAdder();

    private volatile int slabSizeBytes = Integer.getInteger(SLAB_SIZE_PROP, DEFAULT_SLAB_SIZE_BYTES);
    private volatile int maxPooledSlabs = Integer.getInteger(MAX_POOLED_SLABS_PROP, DEFAULT_MAX_POOLED_SLABS);

    private HMAOffHeapArenaPool()
    {
    }

    public static HMAOffHeapArenaPool instance()
    {
        return INSTANCE;
    }

    public HMAOffHeapArena newArena()
    {
        arenasCreated.increment();
        return new HMAOffHeapArena(this, slabSizeBytes);
    }

    public HMAOffHeapArena newArena(int customSlabSize)
    {
        arenasCreated.increment();
        return new HMAOffHeapArena(this, customSlabSize);
    }

    Slab acquireSlab(int requestedSize)
    {
        int standardSize = this.slabSizeBytes;
        if (requestedSize <= standardSize)
        {
            Slab pooled = freeSlabs.poll();
            if (pooled != null)
            {
                pooledSlabsCount.decrementAndGet();
                pooled.reset();
                slabHits.increment();
                activeSlabsCount.incrementAndGet();
                return pooled;
            }
            slabMisses.increment();
            activeSlabsCount.incrementAndGet();
            return allocateNativeSlab(standardSize, true);
        }

        // Oversize allocation: dedicated non-poolable slab
        slabMisses.increment();
        activeSlabsCount.incrementAndGet();
        return allocateNativeSlab(requestedSize, false);
    }

    void releaseSlab(Slab slab)
    {
        if (slab == null)
            return;

        activeSlabsCount.decrementAndGet();
        if (slab.poolable && slab.capacity == this.slabSizeBytes)
        {
            while (true)
            {
                int current = pooledSlabsCount.get();
                if (current >= maxPooledSlabs)
                    break;
                if (pooledSlabsCount.compareAndSet(current, current + 1))
                {
                    slab.reset();
                    freeSlabs.offer(slab);
                    return;
                }
            }
        }
        slab.destroy();
    }

    void onArenaClosed(long bytesAllocatedInArena)
    {
        activeArenaBytes.addAndGet(-bytesAllocatedInArena);
        arenasClosed.increment();
    }

    void onBytesAllocated(long bytes)
    {
        activeArenaBytes.addAndGet(bytes);
        totalBytesAllocated.add(bytes);
    }

    private static Slab allocateNativeSlab(int capacity, boolean poolable)
    {
        if (FFM_AVAILABLE)
        {
            try
            {
                Object arena = FFM_ARENA_OF_SHARED.invoke(null);
                Object segment = FFM_ARENA_ALLOCATE.invoke(arena, (long) capacity, (long) CACHE_LINE_ALIGNMENT);
                long addr = (Long) FFM_SEGMENT_ADDRESS.invoke(segment);
                return new Slab(0L, addr, capacity, arena, poolable);
            }
            catch (Throwable ignored)
            {
                // Fall through to native MemoryUtil allocation
            }
        }

        long raw = MemoryUtil.allocate((long) capacity + CACHE_LINE_ALIGNMENT);
        long aligned = (raw + (CACHE_LINE_ALIGNMENT - 1)) & ~((long) CACHE_LINE_ALIGNMENT - 1);
        return new Slab(raw, aligned, capacity, null, poolable);
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public boolean isFfmAvailable()
    {
        return FFM_AVAILABLE;
    }

    public boolean isFfmArenaAvailable()
    {
        return FFM_AVAILABLE;
    }

    public int getSlabSizeBytes()
    {
        return slabSizeBytes;
    }

    public int slabSizeBytes()
    {
        return slabSizeBytes;
    }

    public int getMaxPooledSlabs()
    {
        return maxPooledSlabs;
    }

    public void setMaxPooledSlabs(int maxPooledSlabs)
    {
        this.maxPooledSlabs = Math.max(0, maxPooledSlabs);
        while (pooledSlabsCount.get() > this.maxPooledSlabs)
        {
            Slab s = freeSlabs.poll();
            if (s == null)
                break;
            pooledSlabsCount.decrementAndGet();
            s.destroy();
        }
    }

    public int getPooledSlabsCount()
    {
        return pooledSlabsCount.get();
    }

    public int pooledSlabsCount()
    {
        return pooledSlabsCount.get();
    }

    public int getActiveSlabsCount()
    {
        return activeSlabsCount.get();
    }

    public int activeSlabsCount()
    {
        return activeSlabsCount.get();
    }

    public long getActiveArenaBytes()
    {
        return Math.max(0L, activeArenaBytes.get());
    }

    public long activeArenaBytes()
    {
        return Math.max(0L, activeArenaBytes.get());
    }

    public long getPooledMemoryBytes()
    {
        return (long) pooledSlabsCount.get() * slabSizeBytes;
    }

    public long getTotalBytesAllocated()
    {
        return totalBytesAllocated.sum();
    }

    public long totalBytesAllocated()
    {
        return totalBytesAllocated.sum();
    }

    public long getSlabHits()
    {
        return slabHits.sum();
    }

    public long slabHits()
    {
        return slabHits.sum();
    }

    public long getSlabMisses()
    {
        return slabMisses.sum();
    }

    public long slabMisses()
    {
        return slabMisses.sum();
    }

    public long getArenasCreated()
    {
        return arenasCreated.sum();
    }

    public long arenasCreated()
    {
        return arenasCreated.sum();
    }

    public long getArenasClosed()
    {
        return arenasClosed.sum();
    }

    public long arenasClosed()
    {
        return arenasClosed.sum();
    }
}
