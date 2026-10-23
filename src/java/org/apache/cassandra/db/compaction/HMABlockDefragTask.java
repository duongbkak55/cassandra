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
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndex;
import org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndexManager;
import org.apache.cassandra.io.sstable.format.hma.HMARecordPointer;
import org.apache.cassandra.io.sstable.format.hma.HMASSTableBlockState;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * Aerospike-inspired Block-Level Defragmentation Task ({@code run_defrag} in {@code drv_ssd.c}).
 *
 * <p>Replaces traditional multi-way Merge-Sort Compaction with a two-tier block reclamation pipeline
 * driven by the off-heap {@link HMAPrimaryIndex} and per-SSTable {@link HMASSTableBlockState}:
 * <ol>
 *   <li><b>Tier 1 — Zero-I/O Direct-Free ({@code n_wblock_direct_frees})</b>:
 *       Any sealed SSTable whose live byte utilization ({@code inuse_sz}) reaches {@code 0}
 *       (all partitions superseded by newer SSTables) is marked obsolete immediately via
 *       {@link CompactionController#getFullyExpiredSSTables()} with <b>zero disk reads and zero disk writes</b>.</li>
 *   <li><b>Tier 2 — LWM Block Defragmentation ({@code run_defrag})</b>:
 *       For SSTables whose live byte utilization drops below {@code defrag-lwm-pct} (e.g. &lt; 50%),
 *       {@link HMADefragScanner} checks each partition key against {@link HMAPrimaryIndex} <i>before</i>
 *       {@code LazilyInitializedUnfilteredRowIterator} seeks or reads {@code Data.db}. Dead/superseded
 *       partitions are skipped in O(1) without touching {@code Data.db} or entering {@code MergeIterator},
 *       copying only still-live partitions forward and reclaiming the fragmented SSTable.</li>
 * </ol>
 */
public class HMABlockDefragTask extends CompactionTask
{
    private static final Logger logger = LoggerFactory.getLogger(HMABlockDefragTask.class);
    private static final ThreadLocal<HMABlockDefragTask> ACTIVE_DEFRAG_TASK = new ThreadLocal<>();

    private final AtomicLong movedRecords = new AtomicLong(0);
    private final AtomicLong skippedRecords = new AtomicLong(0);

    public HMABlockDefragTask(ColumnFamilyStore cfs,
                              ILifecycleTransaction txn,
                              long gcBefore,
                              boolean keepOriginals)
    {
        super(cfs, txn, gcBefore, keepOriginals);
    }

    @Override
    protected CompactionController getCompactionController(Set<SSTableReader> toCompact, long gcBefore)
    {
        ACTIVE_DEFRAG_TASK.set(this);
        return new HMADefragController(cfs, toCompact, gcBefore);
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          ILifecycleTransaction transaction,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new HMADefragCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals, movedRecords, skippedRecords);
    }

    /**
     * If the current thread is executing an {@link HMABlockDefragTask} (or if HMA block defragmentation
     * is globally enabled for the table), wraps the {@link ISSTableScanner} in an {@link HMADefragScanner}
     * so superseded partitions are skipped before touching {@code Data.db}.
     */
    public static ISSTableScanner maybeWrapScanner(ColumnFamilyStore cfs,
                                                   SSTableReader sstable,
                                                   ISSTableScanner delegate)
    {
        HMABlockDefragTask activeTask = ACTIVE_DEFRAG_TASK.get();
        if (activeTask != null)
        {
            return wrapScannerForDefrag(cfs, sstable, delegate, activeTask.movedRecords, activeTask.skippedRecords);
        }
        if (cfs != null && HMABlockCompactionStrategy.isDefragEnabled(cfs.metadata()))
        {
            return wrapScannerForDefrag(cfs, sstable, delegate, null, null);
        }
        return delegate;
    }

    /**
     * Wraps an {@link ISSTableScanner} in an {@link HMADefragScanner} so superseded partitions
     * are filtered out using the off-heap {@link HMAPrimaryIndex} before {@code Data.db} is even read.
     */
    public static ISSTableScanner wrapScannerForDefrag(ColumnFamilyStore cfs,
                                                       SSTableReader sstable,
                                                       ISSTableScanner delegate,
                                                       AtomicLong movedCounter,
                                                       AtomicLong skippedCounter)
    {
        if (cfs == null || sstable == null || delegate == null)
            return delegate;
        if (!HMAPrimaryIndexManager.instance().isEnabled(cfs.metadata()))
            return delegate;
        HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().get(cfs.metadata().id);
        if (index == null)
            return delegate;

        Set<Long> liveCommittedSstableIds = new HashSet<>();
        for (SSTableReader live : cfs.getLiveSSTables())
        {
            liveCommittedSstableIds.add(HMARecordPointer.resolveSSTableId(live.descriptor));
        }

        long sstableId = HMARecordPointer.resolveSSTableId(sstable.descriptor);
        return new HMADefragScanner(delegate, index, sstableId, liveCommittedSstableIds, movedCounter, skippedCounter);
    }

    /**
     * Custom {@link CompactionController} that implements Aerospike's Direct-Free optimization:
     * any SSTable with {@code inuse_sz == 0} ({@link HMASSTableBlockState#isCompletelyDead()})
     * is added to {@code getFullyExpiredSSTables()} and dropped with 0 disk I/O.
     */
    private static class HMADefragController extends CompactionController
    {
        private final Set<SSTableReader> compacting;

        HMADefragController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, long gcBefore)
        {
            super(cfs, compacting, gcBefore);
            this.compacting = compacting;
        }

        @Override
        public Set<SSTableReader> getFullyExpiredSSTables()
        {
            Set<SSTableReader> expired = new HashSet<>(super.getFullyExpiredSSTables());
            if (!HMAPrimaryIndexManager.instance().isEnabled(cfs.metadata()))
                return expired;

            HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().get(cfs.metadata().id);
            if (index == null || compacting == null || compacting.isEmpty())
                return expired;

            // Count how many live SSTables exist on disk so we know newer SSTables are present
            Set<Long> liveSstableIds = new HashSet<>();
            for (SSTableReader live : cfs.getLiveSSTables())
                liveSstableIds.add(HMARecordPointer.resolveSSTableId(live.descriptor));

            for (SSTableReader sstable : compacting)
            {
                if (expired.contains(sstable))
                    continue;

                long sstableId = HMARecordPointer.resolveSSTableId(sstable.descriptor);
                HMASSTableBlockState state = index.getBlockState(sstableId);
                if (state != null && state.isCompletelyDead() && liveSstableIds.size() > 1)
                {
                    expired.add(sstable);
                    long reclaimedBytes = sstable.onDiskLength();
                    index.removeSSTableBlockState(sstableId);
                    index.recordDirectFree(reclaimedBytes);
                    logger.info("HMA Defrag Direct-Free: Dropping 100% dead SSTable {} (sstableId={}, {}) with 0 disk I/O",
                                sstable.getFilename(), sstableId, state);
                }
            }
            return expired;
        }

        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            finally
            {
                ACTIVE_DEFRAG_TASK.remove();
            }
        }
    }

    /**
     * Decorator around {@link ISSTableScanner} that queries the off-heap {@link HMAPrimaryIndex}
     * before initializing each partition's {@code Data.db} reader.
     *
     * <p>Because {@code BigTableScanner} / {@code SSTableScanner} yields a
     * {@code LazilyInitializedUnfilteredRowIterator} that defers {@code Data.db} seeking until
     * {@code initializeIterator()} is invoked, closing a superseded iterator here avoids all
     * {@code Data.db} disk reads, decompression, cell deserialization, and merge-sort CPU overhead.
     */
    public static final class HMADefragScanner extends AbstractIterator<UnfilteredRowIterator> implements ISSTableScanner
    {
        private final ISSTableScanner delegate;
        private final HMAPrimaryIndex index;
        private final long sstableId;
        private final Set<Long> liveCommittedSstableIds;
        private final HMARecordPointer pointer = new HMARecordPointer();
        private final AtomicLong movedCounter;
        private final AtomicLong skippedCounter;

        public HMADefragScanner(ISSTableScanner delegate,
                                HMAPrimaryIndex index,
                                long sstableId,
                                Set<Long> liveCommittedSstableIds,
                                AtomicLong movedCounter,
                                AtomicLong skippedCounter)
        {
            this.delegate = delegate;
            this.index = index;
            this.sstableId = sstableId;
            this.liveCommittedSstableIds = liveCommittedSstableIds;
            this.movedCounter = movedCounter;
            this.skippedCounter = skippedCounter;
        }

        @Override
        protected UnfilteredRowIterator computeNext()
        {
            while (delegate.hasNext())
            {
                UnfilteredRowIterator candidate = delegate.next();
                DecoratedKey key = candidate.partitionKey();

                if (shouldSkipSupersededPartition(key))
                {
                    candidate.close();
                    if (skippedCounter != null)
                        skippedCounter.incrementAndGet();
                    continue;
                }

                if (movedCounter != null)
                    movedCounter.incrementAndGet();
                return candidate;
            }
            return endOfData();
        }

        private boolean shouldSkipSupersededPartition(DecoratedKey key)
        {
            boolean found = index.lookup(key, pointer);
            if (!found)
                return false;

            // If this SSTable holds the latest live record, keep it!
            if (pointer.sstableId == sstableId)
                return false;

            // If the key is fragmented across multiple SSTables (partial row updates), keep it so compaction merges it.
            if (pointer.isMultiSSTable())
                return false;

            // Safe to skip only if the winning SSTable is already committed and live on disk.
            return liveCommittedSstableIds.contains(pointer.sstableId);
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public long getLengthInBytes()
        {
            return delegate.getLengthInBytes();
        }

        @Override
        public long getCompressedLengthInBytes()
        {
            return delegate.getCompressedLengthInBytes();
        }

        @Override
        public long getCurrentPosition()
        {
            return delegate.getCurrentPosition();
        }

        @Override
        public long getBytesScanned()
        {
            return delegate.getBytesScanned();
        }

        @Override
        public Set<SSTableReader> getBackingSSTables()
        {
            return delegate.getBackingSSTables();
        }

        @Override
        public boolean isFullRange()
        {
            return delegate.isFullRange();
        }

        @Override
        public TableMetadata metadata()
        {
            return delegate.metadata();
        }
    }

    /**
     * Defragmentation-aware writer that tracks reclaimed bytes and removes old SSTable block states
     * once the new defragmented SSTable is committed.
     */
    private static class HMADefragCompactionWriter extends DefaultCompactionWriter
    {
        private final HMAPrimaryIndex index;
        private final Set<SSTableReader> originals;
        private final AtomicLong movedRecords;
        private final AtomicLong skippedRecords;
        private final long inputDiskBytes;

        HMADefragCompactionWriter(ColumnFamilyStore cfs,
                                  Directories directories,
                                  ILifecycleTransaction txn,
                                  Set<SSTableReader> nonExpiredSSTables,
                                  boolean keepOriginals,
                                  AtomicLong movedRecords,
                                  AtomicLong skippedRecords)
        {
            super(cfs, directories, txn, nonExpiredSSTables, keepOriginals, 0);
            this.index = HMAPrimaryIndexManager.instance().isEnabled(cfs.metadata())
                         ? HMAPrimaryIndexManager.instance().get(cfs.metadata().id)
                         : null;
            this.originals = nonExpiredSSTables;
            this.movedRecords = movedRecords;
            this.skippedRecords = skippedRecords;
            this.inputDiskBytes = SSTableReader.getTotalBytes(nonExpiredSSTables);
        }

        @Override
        public Collection<SSTableReader> finish()
        {
            Collection<SSTableReader> output = super.finish();
            if (index != null && !originals.isEmpty())
            {
                for (SSTableReader oldSstable : originals)
                {
                    long oldId = HMARecordPointer.resolveSSTableId(oldSstable.descriptor);
                    index.removeSSTableBlockState(oldId);
                }

                long outputDiskBytes = SSTableReader.getTotalBytes(output);
                long reclaimed = Math.max(0L, inputDiskBytes - outputDiskBytes);
                index.recordDefragStats(originals.size(), movedRecords.get(), skippedRecords.get(), reclaimed);

                logger.info("HMA Block Defrag completed for {}.{}: defragged {} SSTables ({} -> {} bytes, reclaimed {} bytes), moved={} live partitions, skipped={} dead partitions",
                            cfs.getKeyspaceName(),
                            cfs.name,
                            originals.size(),
                            inputDiskBytes,
                            outputDiskBytes,
                            reclaimed,
                            movedRecords.get(),
                            skippedRecords.get());
            }
            return output;
        }
    }
}
