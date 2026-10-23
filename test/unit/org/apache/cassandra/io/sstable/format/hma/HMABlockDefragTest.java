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

import java.util.Collection;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class HMABlockDefragTest extends CQLTester
{
    private static DecoratedKey key(String str)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(str));
    }

    @Test
    public void testBlockStateAndDefragQueueTracking()
    {
        try (HMAPrimaryIndex index = new HMAPrimaryIndex(4, 128))
        {
            index.setDefragLwmPct(50);

            long sstable1 = 1001L;
            long sstable2 = 1002L;
            int recordSize = 20_000; // 100 records = ~2 MB across two 1 MB write blocks

            // 1. Write 100 records into SSTable 1
            for (int i = 0; i < 100; i++)
            {
                index.put(key("k_" + i), sstable1, (long) i * recordSize, recordSize, 1000L);
            }
            index.markSSTableSealed(sstable1);

            HMASSTableBlockState state1 = index.getBlockState(sstable1);
            Assert.assertNotNull(state1);
            Assert.assertTrue(state1.isSealed());
            Assert.assertEquals(100L, state1.getTotalRecords());
            Assert.assertEquals(100L, state1.getLiveRecords());
            Assert.assertEquals(100L * recordSize, state1.getWrittenBytes());
            Assert.assertEquals(100L * recordSize, state1.getInUseBytes());
            Assert.assertEquals(100, state1.getLiveUtilizationPct());
            Assert.assertFalse(state1.needsDefrag(50));
            Assert.assertEquals(0, index.getDefragQueueSize());

            // 2. Overwrite 60% of keys (0..59) in SSTable 2 -> SSTable 1 live utilization drops to 40% (< 50% LWM)
            for (int i = 0; i < 60; i++)
            {
                index.put(key("k_" + i), sstable2, (long) i * recordSize, recordSize, 2000L);
            }
            index.markSSTableSealed(sstable2);

            Assert.assertEquals(40L, state1.getLiveRecords());
            Assert.assertEquals(40L * recordSize, state1.getInUseBytes());
            Assert.assertEquals(40, state1.getLiveUtilizationPct());
            Assert.assertTrue(state1.needsDefrag(50));
            Assert.assertFalse(state1.isCompletelyDead());

            // SSTable 1 should have been automatically enqueued into defragQueue
            Long queuedId = index.pollDefragCandidate();
            Assert.assertNotNull(queuedId);
            Assert.assertEquals(sstable1, queuedId.longValue());

            // SSTable 2 has 100% live records and should NOT need defrag
            HMASSTableBlockState state2 = index.getBlockState(sstable2);
            Assert.assertNotNull(state2);
            Assert.assertEquals(60L, state2.getLiveRecords());
            Assert.assertEquals(100, state2.getLiveUtilizationPct());
            Assert.assertFalse(state2.needsDefrag(50));

            // 3. Overwrite remaining 40% of keys (60..99) in SSTable 2 -> SSTable 1 becomes 100% dead (inuse_sz == 0)
            for (int i = 60; i < 100; i++)
            {
                index.put(key("k_" + i), sstable2, (long) i * recordSize, recordSize, 2000L);
            }

            Assert.assertEquals(0L, state1.getLiveRecords());
            Assert.assertEquals(0L, state1.getInUseBytes());
            Assert.assertEquals(0, state1.getLiveUtilizationPct());
            Assert.assertTrue(state1.isCompletelyDead());
            Assert.assertTrue(state1.getZeroUseBlockCount() >= 1);
        }
    }

    @Test
    public void testTier1ZeroIODirectFreeOn100PercentDeadSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) " +
                    "WITH compaction = {'class': 'org.apache.cassandra.db.compaction.HMABlockCompactionStrategy', 'defrag_lwm_pct': '50'}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().getOrCreate(cfs.metadata().id);

        // 1. Insert 80 keys and flush SSTable 1
        for (int i = 0; i < 80; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP 1000", "key_" + i, "val_v1_" + i);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable1 = cfs.getLiveSSTables().iterator().next();
        long sstable1Id = HMARecordPointer.resolveSSTableId(sstable1.descriptor);

        HMASSTableBlockState state1 = index.getBlockState(sstable1Id);
        Assert.assertNotNull(state1);
        Assert.assertEquals(80L, state1.getLiveRecords());
        Assert.assertEquals(100, state1.getLiveUtilizationPct());

        // 2. Overwrite all 80 keys with newer timestamp and flush SSTable 2
        for (int i = 0; i < 80; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP 2000", "key_" + i, "val_v2_" + i);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Assert.assertEquals(2, cfs.getLiveSSTables().size());

        // Verify SSTable 1 is now 100% dead (inuse_sz == 0)
        Assert.assertEquals(0L, state1.getLiveRecords());
        Assert.assertEquals(0L, state1.getInUseBytes());
        Assert.assertTrue(state1.isCompletelyDead());

        long directFreesBefore = index.getDirectFreedSSTableCount();

        // 3. Run HMABlockCompactionStrategy background defrag task
        cfs.getCompactionStrategyManager().enable();
        Collection<AbstractCompactionTask> tasks = cfs.getCompactionStrategyManager().getNextBackgroundTasks(FBUtilities.nowInSeconds());
        Assert.assertEquals(1, tasks.size());

        for (AbstractCompactionTask task : tasks)
        {
            task.execute(ActiveCompactionsTracker.NOOP);
        }

        // Verify Tier 1 Direct-Free reclaimed SSTable 1 with 0 disk writes and left SSTable 2 untouched!
        Assert.assertEquals(directFreesBefore + 1, index.getDirectFreedSSTableCount());
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        Assert.assertNull(index.getBlockState(sstable1Id));

        // Verify all 80 keys return v2
        for (int i = 0; i < 80; i++)
        {
            UntypedResultSet rs = execute("SELECT v FROM %s WHERE k = ?", "key_" + i);
            Assert.assertEquals(1, rs.size());
            Assert.assertEquals("val_v2_" + i, rs.one().getString("v"));
        }
    }

    @Test
    public void testTier2LWMBlockDefragSkipsSupersededRecordsWithoutMergingHotSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) " +
                    "WITH compaction = {'class': 'org.apache.cassandra.db.compaction.HMABlockCompactionStrategy', 'defrag_lwm_pct': '50'}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().getOrCreate(cfs.metadata().id);

        // 1. Insert 100 keys (0..99) into SSTable 1
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP 1000", "key_" + i, "initial_val_" + i);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable1 = cfs.getLiveSSTables().iterator().next();
        long sstable1Id = HMARecordPointer.resolveSSTableId(sstable1.descriptor);

        // 2. Overwrite 70 keys (0..69) in SSTable 2 -> SSTable 1 drops to 30% live (< 50% LWM)
        for (int i = 0; i < 70; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP 2000", "key_" + i, "updated_val_" + i);
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        Assert.assertEquals(2, cfs.getLiveSSTables().size());

        SSTableReader sstable2 = null;
        for (SSTableReader r : cfs.getLiveSSTables())
        {
            if (HMARecordPointer.resolveSSTableId(r.descriptor) != sstable1Id)
                sstable2 = r;
        }
        Assert.assertNotNull(sstable2);
        long sstable2Id = HMARecordPointer.resolveSSTableId(sstable2.descriptor);

        HMASSTableBlockState state1 = index.getBlockState(sstable1Id);
        HMASSTableBlockState state2 = index.getBlockState(sstable2Id);
        Assert.assertNotNull(state1);
        Assert.assertNotNull(state2);

        Assert.assertEquals(30L, state1.getLiveRecords());
        Assert.assertTrue("Expected state1 live utilization < 50%, got " + state1.getLiveUtilizationPct(),
                          state1.getLiveUtilizationPct() < 50);
        Assert.assertTrue(state1.needsDefrag(50));

        Assert.assertEquals(70L, state2.getLiveRecords());
        Assert.assertEquals(100, state2.getLiveUtilizationPct());
        Assert.assertFalse(state2.needsDefrag(50));

        long movedBefore = index.getDefragMovedRecordCount();
        long skippedBefore = index.getDefragSkippedRecordCount();

        // 3. Execute background defrag task
        cfs.getCompactionStrategyManager().enable();
        Collection<AbstractCompactionTask> tasks = cfs.getCompactionStrategyManager().getNextBackgroundTasks(FBUtilities.nowInSeconds());
        Assert.assertEquals(1, tasks.size());
        for (AbstractCompactionTask task : tasks)
        {
            task.execute(ActiveCompactionsTracker.NOOP);
        }

        // Verify HMADefragScanner skipped the 70 superseded records and copied only the 30 live records!
        Assert.assertEquals(skippedBefore + 70L, index.getDefragSkippedRecordCount());
        Assert.assertEquals(movedBefore + 30L, index.getDefragMovedRecordCount());

        // Verify SSTable 1 was obsoleted and SSTable 2 was NEVER rewritten (still in live SSTables!)
        Set<SSTableReader> liveAfter = cfs.getLiveSSTables();
        Assert.assertEquals(2, liveAfter.size());
        Assert.assertTrue("Hot SSTable 2 should not have been rewritten!", liveAfter.contains(sstable2));
        Assert.assertFalse("Fragmented SSTable 1 should have been obsoleted!", liveAfter.contains(sstable1));

        // Verify all 100 keys return exact expected values (0..69 -> updated_val, 70..99 -> initial_val)
        for (int i = 0; i < 100; i++)
        {
            UntypedResultSet rs = execute("SELECT v FROM %s WHERE k = ?", "key_" + i);
            Assert.assertEquals(1, rs.size());
            String expected = i < 70 ? "updated_val_" + i : "initial_val_" + i;
            Assert.assertEquals(expected, rs.one().getString("v"));
        }
    }
}
