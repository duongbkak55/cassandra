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
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Unit tests for HMA Part 3 (Direct I/O & Kernel Bypass + Async I/O Ring)
 * and Part 4 (Streaming Write Buffer / Single-Write CommitLog & Memtable Bypass).
 */
public class HMADirectIOAndStreamingWriteTest extends CQLTester
{
    @After
    public void resetFlags()
    {
        HMADirectIOEngine.instance().setDirectIOEnabled(false);
        HMAWriteBufferManager.instance().setSingleWriteEnabled(false);
    }

    /**
     * Part 3: Verify 4KB hardware sector-aligned buffer allocation, direct read/write channel opening,
     * and non-blocking async batch reads via HMADirectIOEngine.
     */
    @Test
    public void testDirectIOAlignedBuffersAndAsyncBatchReads() throws Exception
    {
        HMADirectIOEngine engine = HMADirectIOEngine.instance();
        engine.resetMetrics();
        engine.setDirectIOEnabled(true);

        // 1. Test 4KB-aligned buffer allocation
        ByteBuffer alignedBuf = engine.allocateAlignedBuffer(5000, 4096);
        Assert.assertEquals(8192, alignedBuf.capacity());
        Assert.assertTrue(alignedBuf.isDirect());

        // 2. Write a test file with 4 distinct 4KB blocks and read them back via async I/O ring
        java.nio.file.Path tempPath = Files.createTempFile("hma_direct_io_test_", ".dat");
        File tempFile = new File(tempPath);
        try
        {
            byte[] fullPayload = new byte[4 * 4096];
            for (int block = 0; block < 4; block++)
            {
                for (int i = 0; i < 4096; i++)
                {
                    fullPayload[block * 4096 + i] = (byte) ((block + 1) * 17 + (i & 0x7F));
                }
            }
            Files.write(tempPath, fullPayload, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

            try (ChannelProxy channel = new ChannelProxy(tempFile,
                    engine.openReadChannelWithFallback(tempFile, StandardOpenOption.READ)))
            {
                long[] offsets = new long[] { 0L, 4096L, 8192L, 12288L };
                int[] lengths = new int[] { 300, 300, 300, 300 };

                List<ByteBuffer> results = engine.readBatchAsync(channel, offsets, lengths, 4096);
                Assert.assertEquals(4, results.size());

                for (int block = 0; block < 4; block++)
                {
                    ByteBuffer slice = results.get(block);
                    Assert.assertEquals(300, slice.remaining());
                    for (int i = 0; i < 300; i++)
                    {
                        byte expected = (byte) ((block + 1) * 17 + (i & 0x7F));
                        Assert.assertEquals(expected, slice.get(i));
                    }
                }
            }

            Assert.assertTrue("Expected directIOReads >= 4", engine.getDirectIOReads() >= 4);
            Assert.assertEquals(4L, engine.getAsyncIOSubmitted());
            Assert.assertEquals(4L, engine.getAsyncIOCompleted());
        }
        finally
        {
            Files.deleteIfExists(tempPath);
        }
    }

    /**
     * Part 3: End-to-end SSTable Flush and Point Lookup using Direct I/O (both compressed and uncompressed SSTables).
     */
    @Test
    public void testDirectIOSSTableWriteAndRead() throws Throwable
    {
        HMADirectIOEngine engine = HMADirectIOEngine.instance();
        engine.resetMetrics();
        engine.setDirectIOEnabled(true);

        // Test 1: Uncompressed SSTable (uses DirectChecksummedSequentialWriter + SimpleChunkReader.Direct)
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfsUncompressed = getCurrentColumnFamilyStore();
        cfsUncompressed.disableAutoCompaction();

        for (int i = 0; i < 50; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", "ukey_" + i, "uval_payload_" + i + "_" + "x".repeat(200));
        }
        cfsUncompressed.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        long writesAfterUncompressed = engine.getDirectIOWrites();
        Assert.assertTrue("Direct I/O writes should be recorded for uncompressed SSTable flush",
                          writesAfterUncompressed > 0);

        for (int i = 0; i < 50; i++)
        {
            UntypedResultSet rs = execute("SELECT v FROM %s WHERE k = ?", "ukey_" + i);
            Assert.assertEquals(1, rs.size());
            Assert.assertEquals("uval_payload_" + i + "_" + "x".repeat(200), rs.one().getString("v"));
        }
        Assert.assertTrue("Direct I/O reads should be recorded for uncompressed SSTable point lookups",
                          engine.getDirectIOReads() > 0);

        // Test 2: Compressed SSTable (uses DirectCompressedSequentialWriter + CompressedChunkReader.Direct)
        long writesBeforeCompressed = engine.getDirectIOWrites();
        long readsBeforeCompressed = engine.getDirectIOReads();

        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text)");
        ColumnFamilyStore cfsCompressed = getCurrentColumnFamilyStore();
        cfsCompressed.disableAutoCompaction();

        for (int i = 0; i < 50; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", "ckey_" + i, "cval_payload_" + i + "_" + "y".repeat(200));
        }
        cfsCompressed.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Assert.assertTrue("Direct I/O writes should increase for compressed SSTable flush",
                          engine.getDirectIOWrites() > writesBeforeCompressed);

        for (int i = 0; i < 50; i++)
        {
            UntypedResultSet rs = execute("SELECT v FROM %s WHERE k = ?", "ckey_" + i);
            Assert.assertEquals(1, rs.size());
            Assert.assertEquals("cval_payload_" + i + "_" + "y".repeat(200), rs.one().getString("v"));
        }
        Assert.assertTrue("Direct I/O reads should increase for compressed SSTable point lookups",
                          engine.getDirectIOReads() > readsBeforeCompressed);
    }

    /**
     * Part 4: Verify Streaming Write Buffer (Single-Write) mode bypasses CommitLog & Memtable,
     * serves zero-latency reads from the active streaming buffer before flush, automatically
     * flushes full streaming blocks to SSTables, and populates the 64-byte HMA Primary Index.
     */
    @Test
    public void testStreamingWriteBufferEliminatesDoubleWrite() throws Throwable
    {
        HMADirectIOEngine.instance().resetMetrics();
        HMADirectIOEngine.instance().setDirectIOEnabled(true);

        HMAWriteBufferManager swbManager = HMAWriteBufferManager.instance();
        swbManager.resetMetrics();

        // Use a small 64 KiB streaming block capacity so automatic streaming flush triggers quickly
        swbManager.setBlockSizeBytes(64 * 1024);

        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Enable Single-Write streaming mode for this table
        swbManager.enableSingleWriteForTable(cfs.metadata().id);

        try
        {
            // Phase 1: Write 5 small records that stay inside the active 64 KiB Streaming Write Buffer
            for (int i = 0; i < 5; i++)
            {
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", "swb_live_" + i, "live_val_" + i);
            }

            // Verify both CommitLog and Memtable were bypassed!
            Assert.assertTrue("CommitLog should be bypassed in Single-Write mode",
                              swbManager.getCommitLogBypassedCount() >= 5);
            Assert.assertTrue("Memtable should be bypassed in Single-Write mode",
                              swbManager.getMemtableBypassedCount() >= 5);
            Assert.assertEquals("No SSTable flush should have occurred yet for 5 tiny rows",
                                0, cfs.getLiveSSTables().size());

            // Verify zero-latency reads directly from the active HMAStreamingWriteBuffer before disk flush
            for (int i = 0; i < 5; i++)
            {
                UntypedResultSet rs = execute("SELECT v FROM %s WHERE k = ?", "swb_live_" + i);
                Assert.assertEquals(1, rs.size());
                Assert.assertEquals("live_val_" + i, rs.one().getString("v"));
            }

            // Overwrite swb_live_0 inside the streaming buffer and verify read-your-writes coalescing
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", "swb_live_0", "live_val_0_updated");
            UntypedResultSet updatedRs = execute("SELECT v FROM %s WHERE k = ?", "swb_live_0");
            Assert.assertEquals("live_val_0_updated", updatedRs.one().getString("v"));

            // Phase 2: Write enough data (~120 KiB) to exceed the 64 KiB streaming buffer and trigger auto-flush
            String largePayload = "Z".repeat(2048);
            for (int i = 0; i < 60; i++)
            {
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", "swb_bulk_" + i, largePayload + "_" + i);
            }

            // Flush any remaining tail in the active streaming buffer
            swbManager.flushTable(cfs);

            Assert.assertTrue("Streaming Write Buffer should have flushed at least 1 SSTable",
                              swbManager.getStreamingBufferFlushes() >= 1);
            Assert.assertTrue("Streaming Write Buffer bytes flushed should be > 0",
                              swbManager.getStreamingBufferBytesFlushed() > 0);

            Set<SSTableReader> liveSSTables = cfs.getLiveSSTables();
            Assert.assertFalse("Live SSTables should exist on disk after streaming flush", liveSSTables.isEmpty());

            // Verify that HMA 64-byte Off-Heap Primary Index was populated during the streaming flush
            HMAPrimaryIndex hmaIndex = HMAPrimaryIndexManager.instance().get(cfs.metadata().id);
            Assert.assertNotNull("HMA 64-byte Primary Index must exist for table", hmaIndex);
            Assert.assertEquals("All 65 unique keys (5 live + 60 bulk) must be indexed in HMA Primary Index",
                                65L, hmaIndex.totalEntries());

            long hitsBefore = hmaIndex.getHitCount();

            // Verify all 65 keys read back accurately from SSTable via 1-I/O HMA Primary Index + Direct I/O
            Assert.assertEquals("live_val_0_updated",
                                execute("SELECT v FROM %s WHERE k = ?", "swb_live_0").one().getString("v"));
            for (int i = 1; i < 5; i++)
            {
                Assert.assertEquals("live_val_" + i,
                                    execute("SELECT v FROM %s WHERE k = ?", "swb_live_" + i).one().getString("v"));
            }
            for (int i = 0; i < 60; i++)
            {
                Assert.assertEquals(largePayload + "_" + i,
                                    execute("SELECT v FROM %s WHERE k = ?", "swb_bulk_" + i).one().getString("v"));
            }

            Assert.assertTrue("HMA Primary Index hits should increase on point lookups",
                              hmaIndex.getHitCount() >= hitsBefore + 65);
        }
        finally
        {
            swbManager.disableSingleWriteForTable(cfs.metadata().id);
            swbManager.setBlockSizeBytes(HMAWriteBufferManager.DEFAULT_SWB_BLOCK_SIZE_BYTES);
        }
    }
}
