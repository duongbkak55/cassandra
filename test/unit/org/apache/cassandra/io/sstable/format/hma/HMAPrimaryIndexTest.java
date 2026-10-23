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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

public class HMAPrimaryIndexTest
{
    private DecoratedKey key(String str)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(str));
    }

    @Test
    public void testBasicPutAndLookup()
    {
        try (HMAPrimaryIndex index = new HMAPrimaryIndex(4, 64))
        {
            DecoratedKey key1 = key("user_session_1001");
            DecoratedKey key2 = key("user_session_1002");

            boolean put1 = index.put(key1, 101L, 4096L, 512, 1000L);
            boolean put2 = index.put(key2, 101L, 4608L, 256, 1001L);

            Assert.assertTrue(put1);
            Assert.assertTrue(put2);

            HMARecordPointer pointer = new HMARecordPointer();

            // Lookup key1
            Assert.assertTrue(index.lookup(key1, pointer));
            Assert.assertEquals(101L, pointer.sstableId);
            Assert.assertEquals(4096L, pointer.dataOffset);
            Assert.assertEquals(512, pointer.dataLength);
            Assert.assertEquals(1000L, pointer.timestamp);

            // Lookup key2
            Assert.assertTrue(index.lookup(key2, pointer));
            Assert.assertEquals(101L, pointer.sstableId);
            Assert.assertEquals(4608L, pointer.dataOffset);
            Assert.assertEquals(256, pointer.dataLength);
            Assert.assertEquals(1001L, pointer.timestamp);

            // Lookup non-existent key
            Assert.assertFalse(index.lookup(key("unknown_key"), pointer));
        }
    }

    @Test
    public void testLastWriteWinsConflictResolution()
    {
        try (HMAPrimaryIndex index = new HMAPrimaryIndex(4, 64))
        {
            DecoratedKey key = key("product_stock_item");

            // Write version 1 with timestamp 500
            index.put(key, 1L, 1024L, 100, 500L);

            HMARecordPointer ptr = new HMARecordPointer();
            Assert.assertTrue(index.lookup(key, ptr));
            Assert.assertEquals(1024L, ptr.dataOffset);

            // Stale write with timestamp 400 (older) -> should be rejected
            boolean staleUpdate = index.put(key, 1L, 2048L, 100, 400L);
            Assert.assertFalse(staleUpdate);

            // Verify unchanged
            Assert.assertTrue(index.lookup(key, ptr));
            Assert.assertEquals(1024L, ptr.dataOffset);

            // Newer write with timestamp 600 -> should update
            boolean newerUpdate = index.put(key, 2L, 4096L, 120, 600L);
            Assert.assertTrue(newerUpdate);

            Assert.assertTrue(index.lookup(key, ptr));
            Assert.assertEquals(2L, ptr.sstableId);
            Assert.assertEquals(4096L, ptr.dataOffset);
            Assert.assertEquals(120, ptr.dataLength);
            Assert.assertEquals(600L, ptr.timestamp);
        }
    }

    @Test
    public void testTombstoneDeletion()
    {
        try (HMAPrimaryIndex index = new HMAPrimaryIndex(4, 64))
        {
            DecoratedKey key = key("delete_me_key");
            index.put(key, 5L, 8192L, 300, 1000L);

            HMARecordPointer ptr = new HMARecordPointer();
            Assert.assertTrue(index.lookup(key, ptr));

            // Delete with timestamp 1100
            index.delete(key, 1100L);

            // Lookup should return false for deleted key
            Assert.assertFalse(index.lookup(key, ptr));
        }
    }

    @Test
    public void testResizingAndHighKeyCount()
    {
        int totalKeys = 10_000;
        try (HMAPrimaryIndex index = new HMAPrimaryIndex(8, 64))
        {
            // Insert 10,000 keys
            for (int i = 0; i < totalKeys; i++)
            {
                DecoratedKey k = key("key_" + i);
                index.put(k, 1L, i * 64L, 48, 1000L + i);
            }

            Assert.assertEquals(totalKeys, index.totalEntries());

            // Verify all 10,000 keys can be looked up
            HMARecordPointer ptr = new HMARecordPointer();
            for (int i = 0; i < totalKeys; i++)
            {
                DecoratedKey k = key("key_" + i);
                boolean found = index.lookup(k, ptr);
                Assert.assertTrue("Key " + i + " must be found", found);
                Assert.assertEquals(i * 64L, ptr.dataOffset);
            }
        }
    }

    @Test
    public void testConcurrentReadersAndWriters() throws InterruptedException
    {
        int threadCount = 8;
        int operationsPerThread = 2000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicBoolean errorFound = new AtomicBoolean(false);

        try (HMAPrimaryIndex index = new HMAPrimaryIndex(16, 256))
        {
            for (int t = 0; t < threadCount; t++)
            {
                final int threadId = t;
                executor.submit(() -> {
                    try
                    {
                        HMARecordPointer ptr = new HMARecordPointer();
                        for (int i = 0; i < operationsPerThread; i++)
                        {
                            DecoratedKey k = key("thread_" + threadId + "_key_" + i);
                            index.put(k, threadId, i * 128L, 64, 1000L + i);

                            // Read back immediately
                            if (!index.lookup(k, ptr))
                            {
                                errorFound.set(true);
                            }
                        }
                    }
                    catch (Throwable ex)
                    {
                        ex.printStackTrace();
                        errorFound.set(true);
                    }
                    finally
                    {
                        latch.countDown();
                    }
                });
            }

            Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
            executor.shutdown();
            Assert.assertFalse("Concurrent execution had errors", errorFound.get());
        }
    }

    @Test
    public void testHMAFlushObserverMultiPartitionAndLWW()
    {
        TableId tableId = TableId.fromUUID(UUID.randomUUID());
        try
        {
            DecoratedKey k1 = key("partition_1");
            DecoratedKey k2 = key("partition_2");
            DecoratedKey k3 = key("partition_3");

            // Simulate flushing SSTable #10 with 3 partitions and explicit data timestamps
            HMAFlushObserver observer1 = new HMAFlushObserver(tableId, 10L);
            observer1.begin();

            observer1.startPartition(k1, 0L, 0L);
            observer1.partitionLevelDeletion(DeletionTime.build(5000L, 100));
            observer1.endPartition(256L, false);

            observer1.startPartition(k2, 256L, 256L);
            observer1.partitionLevelDeletion(DeletionTime.build(6000L, 100));
            observer1.endPartition(1024L, false);

            observer1.startPartition(k3, 1024L, 1024L);
            observer1.partitionLevelDeletion(DeletionTime.build(7000L, 100));
            observer1.endPartition(9216L, true); // wide multi-block partition

            observer1.complete();

            Assert.assertEquals(3, observer1.getIndexedPartitionCount());

            HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().get(tableId);
            Assert.assertNotNull(index);
            Assert.assertEquals(3L, index.totalEntries());

            HMARecordPointer ptr = new HMARecordPointer();

            Assert.assertTrue(index.lookup(k1, ptr));
            Assert.assertEquals(10L, ptr.sstableId);
            Assert.assertEquals(0L, ptr.dataOffset);
            Assert.assertEquals(256, ptr.dataLength);
            Assert.assertEquals(5000L, ptr.timestamp);
            Assert.assertFalse(ptr.isMultiBlock());

            Assert.assertTrue(index.lookup(k2, ptr));
            Assert.assertEquals(10L, ptr.sstableId);
            Assert.assertEquals(256L, ptr.dataOffset);
            Assert.assertEquals(768, ptr.dataLength);
            Assert.assertEquals(6000L, ptr.timestamp);
            Assert.assertFalse(ptr.isMultiBlock());

            Assert.assertTrue(index.lookup(k3, ptr));
            Assert.assertEquals(10L, ptr.sstableId);
            Assert.assertEquals(1024L, ptr.dataOffset);
            Assert.assertEquals(8192, ptr.dataLength);
            Assert.assertEquals(7000L, ptr.timestamp);
            Assert.assertTrue(ptr.isMultiBlock());

            // Simulate older SSTable compaction finishing later with stale timestamp (4000L < 5000L) for k1
            HMAFlushObserver staleCompactionObserver = new HMAFlushObserver(tableId, 11L);
            staleCompactionObserver.begin();
            staleCompactionObserver.startPartition(k1, 512L, 512L);
            staleCompactionObserver.partitionLevelDeletion(DeletionTime.build(4000L, 100));
            staleCompactionObserver.endPartition(700L, false);
            staleCompactionObserver.complete();

            // k1 must still point to SSTable #10 (offset 0) because 5000L > 4000L
            Assert.assertTrue(index.lookup(k1, ptr));
            Assert.assertEquals(10L, ptr.sstableId);
            Assert.assertEquals(0L, ptr.dataOffset);
        }
        finally
        {
            HMAPrimaryIndexManager.instance().remove(tableId);
        }
    }

    @Test
    public void testSSTableIdResolutionAndMetrics()
    {
        SequenceBasedSSTableId seqId = new SequenceBasedSSTableId(42);
        Assert.assertEquals(42L, HMARecordPointer.resolveSSTableId(seqId));

        UUIDBasedSSTableId uuidId = new UUIDBasedSSTableId(TimeUUID.Generator.nextTimeUUID());
        long resolvedUuid = HMARecordPointer.resolveSSTableId(uuidId);
        Assert.assertTrue("UUID-based SSTableId should have MSB set", resolvedUuid < 0);
        Assert.assertEquals(resolvedUuid, HMARecordPointer.resolveSSTableId(uuidId));

        TableId tableId = TableId.fromUUID(UUID.randomUUID());
        try
        {
            HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().getOrCreate(tableId);
            DecoratedKey k = key("metrics_test_key");
            index.put(k, 42L, 128L, 64, 9999L);

            HMARecordPointer ptr = new HMARecordPointer();
            Assert.assertTrue(index.lookup(k, ptr));
            Assert.assertFalse(index.lookup(key("missing_key"), ptr));
            index.recordDirectReadBypass();

            Assert.assertEquals(2L, index.getLookupCount());
            Assert.assertEquals(1L, index.getHitCount());
            Assert.assertEquals(1L, index.getMissCount());
            Assert.assertEquals(1L, index.getDirectReadBypassCount());
        }
        finally
        {
            HMAPrimaryIndexManager.instance().remove(tableId);
        }
    }

    @Test
    public void testBaseAndDeltaTwoSSTableTracking()
    {
        TableId tableId = TableId.fromUUID(UUID.randomUUID());
        try
        {
            HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().getOrCreate(tableId);
            DecoratedKey k = key("base_delta_key");

            // 1. Complete overwrite in Base SSTable #5 at offset 4096
            byte completeFlags = (byte) (HMARecordPointer.FLAG_IN_USE | HMARecordPointer.FLAG_COMPLETE_OVERWRITE | HMARecordPointer.FLAG_BASE_COMPLETE);
            index.put(k, 5L, 4096L, 256, 1000L, 1, completeFlags);

            HMARecordPointer ptr = new HMARecordPointer();
            Assert.assertTrue(index.lookup(k, ptr));
            Assert.assertEquals(5L, ptr.sstableId);
            Assert.assertEquals(4096L, ptr.dataOffset);
            Assert.assertFalse(ptr.isMultiSSTable());
            Assert.assertTrue(ptr.isBaseComplete());

            // 2. Partial update (delta) in SSTable #6 at offset 8192 (without FLAG_COMPLETE_OVERWRITE)
            index.put(k, 6L, 8192L, 64, 2000L, 2, HMARecordPointer.FLAG_IN_USE);

            Assert.assertTrue(index.lookup(k, ptr));
            Assert.assertEquals(6L, ptr.sstableId);
            Assert.assertEquals(8192L, ptr.dataOffset);
            Assert.assertTrue(ptr.isMultiSSTable());
            Assert.assertTrue(ptr.isTwoSSTable());
            Assert.assertTrue(ptr.isBaseComplete());
            Assert.assertEquals(5, ptr.baseSstableId);
            Assert.assertEquals(4096L, ptr.baseDataOffset);

            // 3. Second partial update in SSTable #7 clears FLAG_TWO_SSTABLE while preserving baseSstableId=5 as complete cutoff
            index.put(k, 7L, 12288L, 64, 3000L, 3, HMARecordPointer.FLAG_IN_USE);

            Assert.assertTrue(index.lookup(k, ptr));
            Assert.assertEquals(7L, ptr.sstableId);
            Assert.assertEquals(12288L, ptr.dataOffset);
            Assert.assertTrue(ptr.isMultiSSTable());
            Assert.assertFalse(ptr.isTwoSSTable());
            Assert.assertTrue(ptr.isBaseComplete());
            Assert.assertEquals(5, ptr.baseSstableId);
            Assert.assertEquals(4096L, ptr.baseDataOffset);

            // 4. Verify base SSTable compaction transition (base #5 compacted into #8 while delta #6 is primary)
            DecoratedKey k2 = key("base_compacted_key");
            index.put(k2, 5L, 1024L, 256, 1000L, 1, completeFlags);
            index.put(k2, 6L, 2048L, 64, 2000L, 2, HMARecordPointer.FLAG_IN_USE);
            // Older base #5 (ts=1000) is compacted into SSTable #8 at offset 512
            index.put(k2, 8L, 512L, 256, 1000L, 1, completeFlags);

            Assert.assertTrue(index.lookup(k2, ptr));
            Assert.assertEquals(6L, ptr.sstableId);
            Assert.assertTrue(ptr.isTwoSSTable());
            Assert.assertEquals(8, ptr.baseSstableId);
            Assert.assertEquals(512L, ptr.baseDataOffset);
        }
        finally
        {
            HMAPrimaryIndexManager.instance().remove(tableId);
        }
    }
}
