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

import javax.management.MXBean;

/**
 * JMX MXBean exposing HMA Primary Index metrics via nodetool.
 */
@MXBean
public interface HMAPrimaryIndexMXBean
{
    /**
     * @return Total number of tables with HMA indexes.
     */
    int getIndexedTableCount();

    /**
     * @return Total number of indexed partition key entries across all tables.
     */
    long getTotalIndexedEntries();

    /**
     * @return Total off-heap memory allocated for all HMA indexes in bytes.
     */
    long getTotalOffHeapMemoryBytes();

    /**
     * @return Total off-heap memory allocated for all HMA indexes in megabytes.
     */
    long getTotalOffHeapMemoryMB();

    /**
     * @return Average overhead per indexed entry in bytes (should be close to 64).
     */
    double getAverageBytesPerEntry();

    /**
     * @return Total number of point lookups performed against HMA indexes.
     */
    long getTotalLookups();

    /**
     * @return Total number of successful hits in HMA indexes.
     */
    long getTotalHits();

    /**
     * @return Total number of misses in HMA indexes.
     */
    long getTotalMisses();

    /**
     * @return Hit rate ratio (0.0 to 1.0) across all HMA indexes.
     */
    double getHitRate();

    /**
     * @return Total number of SSTable reads that bypassed Bloom Filter, Key Cache, Index Summary, and Partition Index via HMA.
     */
    long getDirectReadBypassCount();

    /**
     * @return Total number of 100%-dead SSTables reclaimed via Direct-Free with 0 disk I/O.
     */
    long getTotalDirectFreedSSTables();

    /**
     * @return Total number of HMA Block Defragmentation runs completed.
     */
    long getTotalDefragRuns();

    /**
     * @return Total number of SSTables defragmented or direct-freed.
     */
    long getTotalDefraggedSSTables();

    /**
     * @return Total number of live partition records copied forward during block defragmentation.
     */
    long getTotalDefragMovedRecords();

    /**
     * @return Total number of superseded/dead partition records skipped without reading Data.db.
     */
    long getTotalDefragSkippedRecords();

    /**
     * @return Total disk bytes reclaimed by HMA Block Defragmentation and Direct-Free.
     */
    long getTotalDefragReclaimedBytes();

    /**
     * @return Total tracked disk bytes across all active SSTable block states.
     */
    long getTotalTrackedDiskBytes();

    /**
     * @return Total live (in-use) bytes across all active SSTable block states.
     */
    long getTotalTrackedLiveBytes();

    /**
     * @return Global live data utilization percentage (0-100) across all tracked SSTables.
     */
    double getGlobalLiveUtilizationPct();

    // =========================================================================
    // Part 3: Direct I/O & Kernel Bypass Metrics
    // =========================================================================

    boolean isDirectIOEnabled();
    void setDirectIOEnabled(boolean enabled);
    long getDirectIOReads();
    long getDirectIOWrites();
    long getDirectIOBytesRead();
    long getDirectIOBytesWritten();
    long getAsyncIOSubmitted();
    long getAsyncIOCompleted();

    // =========================================================================
    // Part 4: Streaming Write Buffer (Single-Write) Metrics
    // =========================================================================

    boolean isSingleWriteEnabled();
    void setSingleWriteEnabled(boolean enabled);
    long getStreamingBufferFlushes();
    long getCommitLogBypassedCount();
    long getMemtableBypassedCount();
    long getStreamingBufferBytesFlushed();

    // =========================================================================
    // Part 5: Off-Heap Arena Pool & Pluggable Arena Memtable Metrics
    // =========================================================================

    boolean isOffHeapArenaAvailable();
    boolean isFfmArenaSupported();
    int getOffHeapArenaSlabSizeBytes();
    int getOffHeapArenaPooledSlabs();
    int getOffHeapArenaActiveSlabs();
    long getOffHeapArenaActiveBytes();
    long getOffHeapArenaTotalBytesAllocated();
    long getOffHeapArenaSlabHits();
    long getOffHeapArenaSlabMisses();
    long getOffHeapArenaCreatedCount();
    long getOffHeapArenaClosedCount();
    long getOffHeapMemtablePutOps();
    long getOffHeapMemtableDirectLookupHits();
    long getOffHeapMemtableDirectLookupMisses();
}
