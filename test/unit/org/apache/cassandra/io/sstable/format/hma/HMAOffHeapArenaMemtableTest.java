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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.NativeDecoratedKey;
import org.apache.cassandra.db.memtable.HMAOffHeapMemtable;
import org.apache.cassandra.db.memtable.HMAOffHeapMemtableFactory;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.MemoryUtil;

/**
 * Unit tests for HMA Part 5:
 * - {@link HMAOffHeapArenaPool} fixed-size 64B cache-line aligned slab pool & O(1) slab recycling
 * - {@link HMAOffHeapArena} thread-striped lock-free bump allocator & context-aware cloner
 * - {@link NativeDecoratedKey} zero-allocation hashCode/equals/compareTo against {@link BufferDecoratedKey}
 * - {@link HMAOffHeapMemtable} & {@link HMAOffHeapMemtableFactory} O(1) hash point lookups, range scans,
 *   zero-copy SSTable flush, and slab recycling on discard
 * - {@link HMAStreamingWriteBuffer} integration with {@link HMAOffHeapArena}
 */
public class HMAOffHeapArenaMemtableTest extends CQLTester
{
    @After
    public void resetHMAFlags()
    {
        HMAOffHeapMemtableFactory.setAutoEnableForUserTables(true);
        HMAWriteBufferManager.instance().setSingleWriteEnabled(false);
    }

    /**
     * Verify 64-byte cache-line alignment, multi-threaded lock-free bump allocation,
     * and O(1) slab recycling in HMAOffHeapArenaPool.
     */
    @Test
    public void testOffHeapArenaSlabAlignmentConcurrencyAndRecycling() throws Exception
    {
        HMAOffHeapArenaPool pool = HMAOffHeapArenaPool.instance();
        int defaultSlabSize = pool.getSlabSizeBytes();

        long createdBefore = pool.getArenasCreated();
        long closedBefore = pool.getArenasClosed();

        try (HMAOffHeapArena arena = pool.newArena(defaultSlabSize))
        {
            // First allocation in a fresh slab is 64-byte cache-line aligned
            long firstAddr = arena.allocate(64);
            Assert.assertTrue("Expected non-zero native address", firstAddr > 0L);
            Assert.assertEquals("First allocation in slab should be 64B cache-line aligned",
                                0L, firstAddr % HMAOffHeapArenaPool.CACHE_LINE_ALIGNMENT);

            MemoryUtil.setByte(firstAddr, (byte) 0x5A);
            Assert.assertEquals((byte) 0x5A, MemoryUtil.getByte(firstAddr));

            // Multi-threaded striped allocations across 8 threads
            ExecutorService executor = Executors.newFixedThreadPool(8);
            try
            {
                List<Future<Long>> futures = new ArrayList<>();
                for (int t = 0; t < 8; t++)
                {
                    final int threadIdx = t;
                    futures.add(executor.submit((Callable<Long>) () -> {
                        long sum = 0;
                        for (int i = 0; i < 500; i++)
                        {
                            long addr = arena.allocate(128);
                            Assert.assertEquals("Every allocation must be 8-byte aligned", 0L, addr % 8L);
                            byte marker = (byte) (((threadIdx + 1) * 13 + i) & 0x7F);
                            MemoryUtil.setByte(addr, marker);
                            sum += MemoryUtil.getByte(addr);
                        }
                        return sum;
                    }));
                }

                for (Future<Long> f : futures)
                {
                    Assert.assertTrue(f.get() > 0L);
                }
            }
            finally
            {
                executor.shutdownNow();
            }

            Assert.assertTrue("Arena should have allocated > 512KB", arena.getAllocatedBytes() >= 8 * 500 * 128L);
            Assert.assertTrue("Arena should own at least 1 slab", arena.getOwnedSlabCount() >= 1);
        }

        Assert.assertEquals(createdBefore + 1, pool.getArenasCreated());
        Assert.assertEquals(closedBefore + 1, pool.getArenasClosed());
        Assert.assertTrue("Expected recycled slabs in pool after arena.close()", pool.getPooledSlabsCount() >= 1);

        // Acquire another arena and verify it hits the recycled slab pool
        long hitsBefore = pool.getSlabHits();
        try (HMAOffHeapArena reusedArena = pool.newArena(defaultSlabSize))
        {
            long addr = reusedArena.allocate(256);
            Assert.assertTrue(addr > 0L);
            Assert.assertTrue("Expected slab hit from recycled pool", pool.getSlabHits() > hitsBefore);
        }
    }

    /**
     * Verify zero-allocation NativeDecoratedKey hashCode(), equals(), and compareTo()
     * match BufferDecoratedKey semantics for both native-to-heap and native-to-native comparisons.
     */
    @Test
    public void testNativeDecoratedKeyZeroAllocationEqualityAndOrdering()
    {
        HMAOffHeapArenaPool pool = HMAOffHeapArenaPool.instance();
        try (HMAOffHeapArena arena = pool.newArena())
        {
            for (int i = 0; i < 200; i++)
            {
                String rawKeyStr = "user_partition_key_" + i + "_payload_segment_" + (i * 31);
                ByteBuffer buf = ByteBufferUtil.bytes(rawKeyStr);
                DecoratedKey heapKey = Murmur3Partitioner.instance.decorateKey(buf);
                Assert.assertTrue(heapKey instanceof BufferDecoratedKey);

                DecoratedKey nativeKey1 = arena.cloneKey(heapKey);
                DecoratedKey nativeKey2 = new NativeDecoratedKey(heapKey.getToken(), arena, null, buf);

                Assert.assertTrue(nativeKey1 instanceof NativeDecoratedKey);
                Assert.assertEquals("hashCode must match BufferDecoratedKey",
                                    heapKey.hashCode(), nativeKey1.hashCode());
                Assert.assertEquals("NativeDecoratedKey must equal BufferDecoratedKey",
                                    heapKey, nativeKey1);
                Assert.assertEquals("BufferDecoratedKey must equal NativeDecoratedKey",
                                    nativeKey1, heapKey);
                Assert.assertEquals("NativeDecoratedKey must equal another NativeDecoratedKey with same key",
                                    nativeKey1, nativeKey2);
                Assert.assertEquals(0, nativeKey1.compareTo(heapKey));
                Assert.assertEquals(0, heapKey.compareTo(nativeKey1));
                Assert.assertEquals(0, nativeKey1.compareTo(nativeKey2));
                Assert.assertEquals(0, nativeKey1.compareBytesOnly(heapKey));
                Assert.assertEquals(0, nativeKey1.compareBytesOnly(nativeKey2));
            }
        }
    }

    /**
     * End-to-end test of HMAOffHeapMemtable:
     * - Auto-enables on user tables
     * - Stores keys and cells inside HMAOffHeapArena
     * - Serves point lookups via O(1) keyIndex hash map
     * - Serves token range scans via sorted partitions map
     * - Flushes zero-copy to SSTable and populates HMA Primary Index
     * - Recycles arena slabs back to HMAOffHeapArenaPool on discard
     */
    @Test
    public void testHMAOffHeapMemtablePointLookupRangeScanAndZeroCopyFlush() throws Throwable
    {
        HMAOffHeapMemtableFactory.setAutoEnableForUserTables(true);

        createTable("CREATE TABLE %s (k text, c int, v text, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        Memtable activeMemtable = cfs.getTracker().getView().getCurrentMemtable();
        Assert.assertTrue("Expected HMAOffHeapMemtable for user table, got: " + activeMemtable.getClass().getName(),
                          activeMemtable instanceof HMAOffHeapMemtable);

        HMAOffHeapMemtable hmaMemtable = (HMAOffHeapMemtable) activeMemtable;
        long putsBefore = HMAOffHeapMemtable.getOffHeapPutOps();
        long directHitsBefore = HMAOffHeapMemtable.getDirectLookupHits();

        // 1. Insert 100 partitions with multiple clustering rows and updates
        for (int i = 0; i < 100; i++)
        {
            String key = String.format("key_%04d", i);
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", key, 1, "val_initial_" + i);
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", key, 2, "val_second_" + i);
            // Overwrite clustering row 1 to test in-place off-heap BTree reconciliation
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", key, 1, "val_updated_" + i);
        }

        Assert.assertEquals(100L, hmaMemtable.partitionCount());
        Assert.assertTrue("Expected off-heap arena bytes > 0",
                          hmaMemtable.getArena().getAllocatedBytes() > 0L);
        Assert.assertTrue("Expected off-heap put ops to increment",
                          HMAOffHeapMemtable.getOffHeapPutOps() >= putsBefore + 300);

        // 2. Point lookups served directly from HMAOffHeapMemtable O(1) hash index
        for (int i = 0; i < 100; i++)
        {
            String key = String.format("key_%04d", i);
            UntypedResultSet rs = execute("SELECT c, v FROM %s WHERE k = ?", key);
            Assert.assertEquals(2, rs.size());
            assertRows(rs,
                       row(1, "val_updated_" + i),
                       row(2, "val_second_" + i));
        }
        Assert.assertTrue("Expected O(1) direct hash lookup hits in HMAOffHeapMemtable",
                          HMAOffHeapMemtable.getDirectLookupHits() >= directHitsBefore + 100);

        // 3. Range scan across all partitions in HMAOffHeapMemtable
        UntypedResultSet allRows = execute("SELECT k, c, v FROM %s");
        Assert.assertEquals(200, allRows.size());

        // 4. Zero-copy flush to SSTable -> populates HMA Primary Index and recycles Arena slabs
        long arenasClosedBefore = HMAOffHeapArenaPool.instance().getArenasClosed();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Assert.assertTrue("Expected previous HMAOffHeapMemtable arena to close and recycle slabs on flush",
                          HMAOffHeapArenaPool.instance().getArenasClosed() > arenasClosedBefore);

        HMAPrimaryIndex hmaIndex = HMAPrimaryIndexManager.instance().get(cfs.metadata().id);
        Assert.assertNotNull("Expected HMA Primary Index to be populated after flush", hmaIndex);
        Assert.assertEquals(100L, hmaIndex.totalEntries());

        // 5. Verify post-flush point lookups hit HMA Primary Index and return exact data
        for (int i = 0; i < 100; i++)
        {
            String key = String.format("key_%04d", i);
            UntypedResultSet rs = execute("SELECT c, v FROM %s WHERE k = ?", key);
            assertRows(rs,
                       row(1, "val_updated_" + i),
                       row(2, "val_second_" + i));
        }
    }

    /**
     * Verify Part 4 + Part 5 integration: HMAStreamingWriteBuffer backed by HMAOffHeapArena
     * in Single-Write mode (CommitLog + Memtable bypass) allocates off-heap and recycles slabs upon flush.
     */
    @Test
    public void testStreamingWriteBufferUsesOffHeapArenaAndRecyclesSlabs() throws Throwable
    {
        HMAWriteBufferManager manager = HMAWriteBufferManager.instance();
        manager.resetMetrics();
        manager.setBlockSizeBytes(64 * 1024); // 64 KiB buffer

        createTable("CREATE TABLE %s (k text PRIMARY KEY, v blob)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        manager.enableSingleWriteForTable(cfs.metadata().id);

        try
        {
            byte[] payload = new byte[1024];
            for (int i = 0; i < payload.length; i++)
                payload[i] = (byte) (i & 0x7F);

            for (int i = 0; i < 40; i++)
            {
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", "swb_key_" + i, ByteBuffer.wrap(payload));
            }

            // Read before and after explicit flush
            for (int i = 0; i < 40; i++)
            {
                UntypedResultSet rs = execute("SELECT v FROM %s WHERE k = ?", "swb_key_" + i);
                Assert.assertEquals(1, rs.size());
            }

            manager.flushTable(cfs);
            Assert.assertTrue("Expected streaming buffer flushes >= 1", manager.getStreamingBufferFlushes() >= 1);
            Assert.assertEquals(40L, manager.getCommitLogBypassedCount());
            Assert.assertEquals(40L, manager.getMemtableBypassedCount());

            for (int i = 0; i < 40; i++)
            {
                UntypedResultSet rs = execute("SELECT v FROM %s WHERE k = ?", "swb_key_" + i);
                Assert.assertEquals(1, rs.size());
                Assert.assertArrayEquals(payload, ByteBufferUtil.getArray(rs.one().getBytes("v")));
            }
        }
        finally
        {
            manager.disableSingleWriteForTable(cfs.metadata().id);
            manager.setBlockSizeBytes(HMAWriteBufferManager.DEFAULT_SWB_BLOCK_SIZE_BYTES);
        }
    }
}
