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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * SSTableFlushObserver that populates the HMA Primary Index during SSTable writes.
 *
 * <p>This observer is attached to the SSTable writer pipeline. As each partition is
 * written to the SSTable data file, the observer records its key hash, file position,
 * exact serialized data length, and maximum data timestamp into the off-heap HMA index.
 * This enables subsequent point reads to bypass the entire Bloom Filter → Key Cache →
 * Partition Summary → Partition Index chain and resolve key locations with a single
 * O(1) hash lookup in off-heap RAM.
 *
 * <h3>Integration Points</h3>
 * <ul>
 *   <li>Memtable flush → SSTable write: each partition's position is indexed</li>
 *   <li>Compaction output: compacted SSTable partitions are re-indexed with updated positions and LWW timestamps</li>
 *   <li>Streaming: incoming SSTables are indexed on arrival</li>
 * </ul>
 */
public class HMAFlushObserver implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(HMAFlushObserver.class);

    private final HMAPrimaryIndex index;
    private final long sstableId;
    private final TableId tableId;
    private final int expectedSingleRowColumns;

    // State during partition write
    private DecoratedKey currentKey;
    private long currentKeyPosition;
    private long currentMaxTimestamp = Long.MIN_VALUE;
    private boolean currentIsMultiBlock = false;
    private boolean currentHasPrimaryKeyLiveness = false;
    private boolean currentHasPartitionDeletion = false;
    private int currentColumnCount = 0;
    private int partitionCount;

    public HMAFlushObserver(TableId tableId, long sstableId)
    {
        this(tableId, sstableId, -1);
    }

    public HMAFlushObserver(TableId tableId, long sstableId, int expectedSingleRowColumns)
    {
        this.tableId = tableId;
        this.sstableId = sstableId;
        this.index = HMAPrimaryIndexManager.instance().getOrCreate(tableId);
        this.expectedSingleRowColumns = expectedSingleRowColumns;
        this.partitionCount = 0;
    }

    public HMAFlushObserver(TableId tableId, Descriptor descriptor)
    {
        this(tableId, HMARecordPointer.resolveSSTableId(descriptor), -1);
    }

    public HMAFlushObserver(TableMetadata metadata, Descriptor descriptor)
    {
        this(metadata.id,
             HMARecordPointer.resolveSSTableId(descriptor),
             metadata.clusteringColumns().isEmpty() ? metadata.regularAndStaticColumns().size() : -2);
    }

    @Override
    public void begin()
    {
        // nothing to do
    }

    /**
     * Called when a new partition is being written. If a previous partition was not yet
     * finalized via {@link #endPartition(long, boolean)}, flush it first using the delta
     * between key positions as its serialized length.
     */
    @Override
    public void startPartition(DecoratedKey key, long keyPosition, long keyPositionForSASI)
    {
        if (this.currentKey != null)
        {
            int length = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, keyPosition - this.currentKeyPosition));
            flushCurrentPartition(length);
        }

        this.currentKey = key;
        this.currentKeyPosition = keyPosition;
        this.currentMaxTimestamp = Long.MIN_VALUE;
        this.currentIsMultiBlock = false;
        this.currentHasPrimaryKeyLiveness = false;
        this.currentHasPartitionDeletion = false;
        this.currentColumnCount = 0;
    }

    @Override
    public void partitionLevelDeletion(DeletionTime deletionTime)
    {
        if (deletionTime != null && !deletionTime.isLive())
        {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, deletionTime.markedForDeleteAt());
            currentHasPartitionDeletion = true;
        }
    }

    @Override
    public void staticRow(Row staticRow)
    {
        if (staticRow != null && !staticRow.isEmpty())
        {
            updateTimestampFromRow(staticRow);
        }
    }

    @Override
    public void nextUnfilteredCluster(Unfiltered unfiltered)
    {
        if (unfiltered == null)
            return;

        if (unfiltered.isRow())
        {
            updateTimestampFromRow((Row) unfiltered);
        }
        else if (unfiltered.isRangeTombstoneMarker())
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
            if (marker.isBoundary())
            {
                RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) marker;
                if (!bm.endDeletionTime().isLive())
                    currentMaxTimestamp = Math.max(currentMaxTimestamp, bm.endDeletionTime().markedForDeleteAt());
                if (!bm.startDeletionTime().isLive())
                    currentMaxTimestamp = Math.max(currentMaxTimestamp, bm.startDeletionTime().markedForDeleteAt());
            }
            else
            {
                DeletionTime dt = ((RangeTombstoneBoundMarker) marker).deletionTime();
                if (!dt.isLive())
                    currentMaxTimestamp = Math.max(currentMaxTimestamp, dt.markedForDeleteAt());
            }
        }
    }

    @Override
    public void endPartition(long endPosition, boolean isIndexed)
    {
        if (this.currentKey == null)
            return;

        this.currentIsMultiBlock = isIndexed;
        int length = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, endPosition - this.currentKeyPosition));
        flushCurrentPartition(length);
    }

    private void updateTimestampFromRow(Row row)
    {
        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        if (!liveness.isEmpty())
        {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, liveness.timestamp());
            currentHasPrimaryKeyLiveness = true;
        }

        DeletionTime rowDeletion = row.deletion().time();
        if (!rowDeletion.isLive())
        {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, rowDeletion.markedForDeleteAt());
            currentHasPartitionDeletion = true;
        }

        for (ColumnData cd : row)
        {
            if (cd.column().isSimple())
            {
                currentColumnCount++;
                currentMaxTimestamp = Math.max(currentMaxTimestamp, ((Cell<?>) cd).timestamp());
            }
            else
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                if (!complex.complexDeletion().isLive() || complex.cellsCount() > 0)
                    currentColumnCount++;
                if (!complex.complexDeletion().isLive())
                    currentMaxTimestamp = Math.max(currentMaxTimestamp, complex.complexDeletion().markedForDeleteAt());
                for (Cell<?> cell : complex)
                    currentMaxTimestamp = Math.max(currentMaxTimestamp, cell.timestamp());
            }
        }
    }

    /**
     * Called when the SSTable is fully written. We finalize indexing of the last partition
     * if needed, and log summary statistics.
     */
    @Override
    public void complete()
    {
        // Flush the last buffered partition if present
        flushCurrentPartition(0);
        index.markSSTableSealed(sstableId);

        if (logger.isDebugEnabled())
        {
            logger.debug("HMA: Indexed {} partitions from SSTable {} for table {} " +
                         "(total index entries={}, offheap={}KB)",
                         partitionCount,
                         sstableId,
                         tableId,
                         index.totalEntries(),
                         index.offHeapMemoryAllocated() / 1024);
        }
    }

    /**
     * Called when SSTable writer switches during sharded compaction.
     * We flush the current partition to index before the switch.
     */
    @Override
    public void onSSTableWriterSwitched()
    {
        flushCurrentPartition(0);
    }

    @Override
    public void abort(Throwable accumulator)
    {
        currentKey = null;
        currentMaxTimestamp = Long.MIN_VALUE;
        currentIsMultiBlock = false;
        currentHasPrimaryKeyLiveness = false;
        currentColumnCount = 0;
    }

    public int getIndexedPartitionCount()
    {
        return partitionCount;
    }

    /**
     * Flush the current partition into the HMA index.
     *
     * @param estimatedLength serialized byte length of the partition data in Data.db.
     */
    private void flushCurrentPartition(int estimatedLength)
    {
        if (currentKey == null)
            return;

        int dataLength = Math.max(0, estimatedLength);
        long timestamp = currentMaxTimestamp != Long.MIN_VALUE
                         ? currentMaxTimestamp
                         : System.currentTimeMillis() * 1000L;

        byte flags = currentIsMultiBlock
                     ? (byte) (HMARecordPointer.FLAG_IN_USE | HMARecordPointer.FLAG_MULTI_BLOCK)
                     : HMARecordPointer.FLAG_IN_USE;

        if (expectedSingleRowColumns == -1 ||
            (expectedSingleRowColumns >= 0 &&
             (currentHasPartitionDeletion ||
              currentColumnCount >= expectedSingleRowColumns ||
              (currentHasPrimaryKeyLiveness && expectedSingleRowColumns > 2 && currentColumnCount >= expectedSingleRowColumns - 1))))
        {
            flags |= HMARecordPointer.FLAG_COMPLETE_OVERWRITE;
        }

        index.put(currentKey,
                  sstableId,
                  currentKeyPosition,
                  dataLength,
                  timestamp,
                  0,
                  flags);

        partitionCount++;
        currentKey = null;
        currentMaxTimestamp = Long.MIN_VALUE;
        currentIsMultiBlock = false;
        currentHasPrimaryKeyLiveness = false;
        currentHasPartitionDeletion = false;
        currentColumnCount = 0;
    }
}
