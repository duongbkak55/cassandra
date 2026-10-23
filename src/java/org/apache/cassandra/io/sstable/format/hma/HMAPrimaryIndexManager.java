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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Singleton manager for HMA Primary Indexes, one per table (ColumnFamily).
 *
 * <p>This manager follows the lifecycle of Cassandra tables. When a table is created
 * or loaded, an HMAPrimaryIndex is allocated. When a table is dropped, the index
 * is released and its off-heap memory is freed.
 *
 * <p>The manager also exposes global metrics: total off-heap bytes allocated,
 * total indexed entries, and per-table statistics.
 */
public final class HMAPrimaryIndexManager
{
    private static final Logger logger = LoggerFactory.getLogger(HMAPrimaryIndexManager.class);

    private static final HMAPrimaryIndexManager INSTANCE = new HMAPrimaryIndexManager();

    /**
     * Default number of shards per table index.
     * Tunable via cassandra.yaml or system property in a future iteration.
     */
    private static final int DEFAULT_SHARDS = Math.max(16,
            Runtime.getRuntime().availableProcessors() * 4);

    /**
     * Default initial hash table capacity per shard.
     * Chosen to avoid excessive early resizing for tables with typical partition counts.
     */
    private static final int DEFAULT_INITIAL_CAPACITY_PER_SHARD = 4096;

    /**
     * Global feature flag for HMA Primary Index (defaults to true, configurable via -Dcassandra.hma.enabled).
     */
    private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty("cassandra.hma.enabled", "true"));

    /**
     * Whether to also index system keyspaces (defaults to false to save off-heap memory).
     */
    private static final boolean INDEX_SYSTEM_KEYSPACES = Boolean.parseBoolean(System.getProperty("cassandra.hma.index_system_keyspaces", "false"));

    private final ConcurrentHashMap<TableId, HMAPrimaryIndex> indexes = new ConcurrentHashMap<>();

    static
    {
        HMAPrimaryIndexMetrics.register();
    }

    private HMAPrimaryIndexManager()
    {
    }

    public static HMAPrimaryIndexManager instance()
    {
        return INSTANCE;
    }

    /**
     * Check whether HMA is globally enabled via system property (-Dcassandra.hma.enabled).
     */
    public boolean isEnabled()
    {
        return ENABLED;
    }

    /**
     * Check whether HMA off-heap primary indexing is enabled for the given table.
     */
    public boolean isEnabled(TableMetadata metadata)
    {
        if (!ENABLED || metadata == null)
            return false;
        if (!INDEX_SYSTEM_KEYSPACES &&
            (SchemaConstants.isLocalSystemKeyspace(metadata.keyspace) ||
             SchemaConstants.isReplicatedSystemKeyspace(metadata.keyspace)))
        {
            return false;
        }
        return true;
    }

    /**
     * Get or create the HMA Primary Index for the given table.
     */
    public HMAPrimaryIndex getOrCreate(TableId tableId)
    {
        return indexes.computeIfAbsent(tableId, id ->
        {
            logger.info("HMA: Creating off-heap primary index for table {} with {} shards, {} capacity/shard",
                        id, DEFAULT_SHARDS, DEFAULT_INITIAL_CAPACITY_PER_SHARD);
            return new HMAPrimaryIndex(DEFAULT_SHARDS, DEFAULT_INITIAL_CAPACITY_PER_SHARD);
        });
    }

    /**
     * Get the existing index for a table, or null if none exists.
     */
    public HMAPrimaryIndex get(TableId tableId)
    {
        return indexes.get(tableId);
    }

    /**
     * Check if a table has an HMA index.
     */
    public boolean hasIndex(TableId tableId)
    {
        return indexes.containsKey(tableId);
    }

    /**
     * Remove and close the HMA index for a dropped table, freeing off-heap memory.
     */
    public void remove(TableId tableId)
    {
        HMAFastCasEngine.bumpCacheEpoch();
        HMAWriteBufferManager.instance().clearTable(tableId);
        HMAPrimaryIndex removed = indexes.remove(tableId);
        if (removed != null)
        {
            logger.info("HMA: Closing off-heap primary index for table {} " +
                        "(entries={}, offheap={}MB)",
                        tableId,
                        removed.totalEntries(),
                        removed.offHeapMemoryAllocated() / (1024 * 1024));
            removed.close();
        }
    }

    /**
     * Total off-heap memory allocated across all table indexes.
     */
    public long totalOffHeapMemory()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.offHeapMemoryAllocated();
        return total;
    }

    /**
     * Total indexed entries across all tables.
     */
    public long totalEntries()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.totalEntries();
        return total;
    }

    public long totalLookups()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getLookupCount();
        return total;
    }

    public long totalHits()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getHitCount();
        return total;
    }

    public long totalMisses()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getMissCount();
        return total;
    }

    public long totalDirectReadBypasses()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getDirectReadBypassCount();
        return total;
    }

    public long totalDirectFreedSSTables()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getDirectFreedSSTableCount();
        return total;
    }

    public long totalDefragRuns()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getDefragRunCount();
        return total;
    }

    public long totalDefraggedSSTables()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getDefraggedSSTableCount();
        return total;
    }

    public long totalDefragMovedRecords()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getDefragMovedRecordCount();
        return total;
    }

    public long totalDefragSkippedRecords()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getDefragSkippedRecordCount();
        return total;
    }

    public long totalDefragReclaimedBytes()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getDefragReclaimedByteCount();
        return total;
    }

    public long totalTrackedDiskBytes()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getTotalTrackedDiskBytes();
        return total;
    }

    public long totalTrackedLiveBytes()
    {
        long total = 0;
        for (HMAPrimaryIndex idx : indexes.values())
            total += idx.getTotalTrackedLiveBytes();
        return total;
    }

    /**
     * Populate the HMA Primary Index from a collection of existing SSTables (e.g., on table startup or import).
     */
    public void populateFromSSTables(TableMetadata metadata, Collection<SSTableReader> sstables)
    {
        if (!isEnabled(metadata) || sstables == null || sstables.isEmpty())
            return;

        for (SSTableReader sstable : sstables)
        {
            populateFromSSTable(sstable);
        }
    }

    /**
     * Populate the HMA Primary Index from an existing SSTableReader by scanning its partition keys.
     */
    public void populateFromSSTable(SSTableReader sstable)
    {
        if (sstable == null || !isEnabled(sstable.metadata()))
            return;

        HMAPrimaryIndex index = getOrCreate(sstable.metadata().id);
        long sstableId = HMARecordPointer.resolveSSTableId(sstable.descriptor);
        long maxTimestamp = sstable.getMaxTimestamp();
        long uncompLen = sstable.uncompressedLength();

        boolean singleRowTable = sstable.metadata().clusteringColumns().isEmpty();
        byte baseFlags = singleRowTable
                         ? (byte) (HMARecordPointer.FLAG_IN_USE | HMARecordPointer.FLAG_COMPLETE_OVERWRITE)
                         : HMARecordPointer.FLAG_IN_USE;

        try (KeyReader keyReader = sstable.keyReader(true))
        {
            DecoratedKey prevKey = null;
            long prevPos = -1;
            byte prevFlags = baseFlags;

            while (!keyReader.isExhausted())
            {
                long pos = keyReader.dataPosition();
                DecoratedKey currentKey = sstable.decorateKey(keyReader.key());
                boolean isMultiBlock = keyReader.isIndexed();

                if (prevKey != null && prevPos >= 0)
                {
                    int len = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, pos - prevPos));
                    index.put(prevKey, sstableId, prevPos, len, maxTimestamp, 0, prevFlags);
                }

                prevKey = currentKey;
                prevPos = pos;
                prevFlags = isMultiBlock
                            ? (byte) (baseFlags | HMARecordPointer.FLAG_MULTI_BLOCK)
                            : baseFlags;

                if (!keyReader.advance())
                    break;
            }

            if (prevKey != null && prevPos >= 0)
            {
                int len = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, uncompLen - prevPos));
                index.put(prevKey, sstableId, prevPos, len, maxTimestamp, 0, prevFlags);
            }
            index.markSSTableSealed(sstableId);
        }
        catch (IOException e)
        {
            logger.warn("HMA: Failed to populate primary index from SSTable {}", sstable.getFilename(), e);
        }
    }

    /**
     * Number of tables currently using HMA indexes.
     */
    public int indexedTableCount()
    {
        return indexes.size();
    }

    /**
     * Shut down all indexes. Called during node shutdown.
     */
    public void closeAll()
    {
        logger.info("HMA: Shutting down all primary indexes ({} tables, {}MB off-heap)",
                    indexes.size(),
                    totalOffHeapMemory() / (1024 * 1024));

        for (TableId tableId : indexes.keySet())
        {
            remove(tableId);
        }
    }
}
