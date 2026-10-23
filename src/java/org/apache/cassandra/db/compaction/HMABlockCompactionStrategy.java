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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndex;
import org.apache.cassandra.io.sstable.format.hma.HMAPrimaryIndexManager;
import org.apache.cassandra.io.sstable.format.hma.HMARecordPointer;
import org.apache.cassandra.io.sstable.format.hma.HMASSTableBlockState;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;

import static com.google.common.collect.Iterables.filter;

/**
 * Aerospike-inspired Block-Level Defragmentation Strategy ({@code run_defrag} in {@code drv_ssd.c}).
 *
 * <p>Unlike {@link SizeTieredCompactionStrategy} or {@link LeveledCompactionStrategy}, which repeatedly
 * merge-sort 100%-live SSTables and suffer 10x–30x Write Amplification, {@code HMABlockCompactionStrategy}
 * relies on the off-heap {@link HMAPrimaryIndex} for O(1) point reads across any number of SSTables and
 * only touches an SSTable when its live byte utilization ({@code inuse_sz}) drops below
 * {@code defrag_lwm_pct} (default 50%):
 * <ul>
 *   <li>SSTables with {@code inuse_sz == 0} (0% live) are reclaimed via <b>Direct-Free</b> with 0 disk I/O.</li>
 *   <li>SSTables with {@code 0 < inuse_pct < defrag_lwm_pct} are scanned via {@link HMABlockDefragTask.HMADefragScanner},
 *       skipping dead/superseded partitions without seeking {@code Data.db} and copying only live partitions forward.</li>
 *   <li>SSTables with {@code inuse_pct >= defrag_lwm_pct} are left untouched on disk (0 Write Amplification).</li>
 * </ul>
 */
public class HMABlockCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(HMABlockCompactionStrategy.class);

    public static final String DEFRAG_LWM_PCT_OPTION = "defrag_lwm_pct";
    public static final String MAX_DEFRAG_SSTABLES_OPTION = "max_defrag_sstables";

    private static final boolean DEFRAG_ENABLED = Boolean.parseBoolean(
        System.getProperty("cassandra.hma.defrag.enabled", "true"));
    private static final boolean REPLACE_STCS_MERGE = Boolean.parseBoolean(
        System.getProperty("cassandra.hma.defrag.replace_stcs", "false"));

    private final Set<SSTableReader> sstables = new HashSet<>();
    private final int defragLwmPct;
    private final int maxDefragSSTables;
    private volatile int estimatedRemainingTasks = 0;

    public HMABlockCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        int lwm = HMAPrimaryIndex.DEFAULT_DEFRAG_LWM_PCT;
        int maxBatch = Math.max(4, cfs.getMaximumCompactionThreshold());

        if (options != null)
        {
            if (options.containsKey(DEFRAG_LWM_PCT_OPTION))
                lwm = Integer.parseInt(options.get(DEFRAG_LWM_PCT_OPTION));
            if (options.containsKey(MAX_DEFRAG_SSTABLES_OPTION))
                maxBatch = Integer.parseInt(options.get(MAX_DEFRAG_SSTABLES_OPTION));
        }

        this.defragLwmPct = Math.max(1, Math.min(99, lwm));
        this.maxDefragSSTables = Math.max(1, maxBatch);

        if (HMAPrimaryIndexManager.instance().isEnabled(cfs.metadata()))
        {
            HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().getOrCreate(cfs.metadata().id);
            index.setDefragLwmPct(this.defragLwmPct);
        }
    }

    public static boolean isDefragEnabled(TableMetadata metadata)
    {
        return DEFRAG_ENABLED && HMAPrimaryIndexManager.instance().isEnabled(metadata);
    }

    public static boolean shouldReplaceSTCSMerge()
    {
        return REPLACE_STCS_MERGE;
    }

    /**
     * Find sealed SSTables whose live byte utilization ({@code inuse_sz / written_sz}) is strictly below {@code lwmPct},
     * ordered from lowest live utilization (0% dead blocks first) to highest.
     */
    public static List<SSTableReader> findDefragCandidates(ColumnFamilyStore cfs,
                                                           Iterable<SSTableReader> uncompactingCandidates,
                                                           int lwmPct,
                                                           int maxBatchSize)
    {
        if (!isDefragEnabled(cfs.metadata()))
            return Collections.emptyList();

        HMAPrimaryIndex index = HMAPrimaryIndexManager.instance().get(cfs.metadata().id);
        if (index == null)
            return Collections.emptyList();

        // Drain queued notifications from index.defragQueue
        while (index.pollDefragCandidate() != null)
        {
            // We inspect all uncompacting candidates against their live block state below
        }

        Map<SSTableReader, Integer> candidateUtilization = new HashMap<>();
        int totalLiveSSTables = cfs.getLiveSSTables().size();

        for (SSTableReader sstable : uncompactingCandidates)
        {
            long sstableId = HMARecordPointer.resolveSSTableId(sstable.descriptor);
            HMASSTableBlockState state = index.getBlockState(sstableId);
            if (state == null)
                continue;

            // Ensure sealed state for committed live SSTables
            if (!state.isSealed())
                state.markSealed();

            if (state.needsDefrag(lwmPct))
            {
                // Avoid direct-freeing the sole SSTable on disk if no newer SSTable exists
                if (state.isCompletelyDead() && totalLiveSSTables <= 1)
                    continue;

                candidateUtilization.put(sstable, state.getLiveUtilizationPct());
            }
        }

        if (candidateUtilization.isEmpty())
            return Collections.emptyList();

        List<SSTableReader> sorted = new ArrayList<>(candidateUtilization.keySet());
        sorted.sort(Comparator.<SSTableReader>comparingInt(candidateUtilization::get)
                              .thenComparingLong(SSTableReader::onDiskLength));

        if (sorted.size() > maxBatchSize)
            return new ArrayList<>(sorted.subList(0, maxBatchSize));
        return sorted;
    }

    private synchronized List<SSTableReader> getNextDefragSSTables(final long gcBefore)
    {
        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));
        List<SSTableReader> defragBatch = findDefragCandidates(cfs, candidates, defragLwmPct, maxDefragSSTables);
        if (!defragBatch.isEmpty())
        {
            estimatedRemainingTasks = Math.max(1, defragBatch.size() / Math.max(1, maxDefragSSTables));
            return defragBatch;
        }

        estimatedRemainingTasks = 0;
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                return Collections.singletonList(sstable);
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<AbstractCompactionTask> getNextBackgroundTasks(long gcBefore)
    {
        List<SSTableReader> previousCandidate = null;
        while (true)
        {
            List<SSTableReader> candidates = getNextDefragSSTables(gcBefore);
            if (candidates.isEmpty())
                return Collections.emptyList();

            if (candidates.equals(previousCandidate))
                return Collections.emptyList();

            ILifecycleTransaction transaction = cfs.getTracker().tryModify(candidates, OperationType.COMPACTION);
            if (transaction != null)
            {
                return Collections.singletonList(new HMABlockDefragTask(cfs, transaction, gcBefore, false));
            }
            previousCandidate = candidates;
        }
    }

    @Override
    public synchronized List<AbstractCompactionTask> getMaximalTasks(final long gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;

        // First check if only fragmented SSTables need defragmentation
        List<SSTableReader> defragCandidates = findDefragCandidates(cfs, filteredSSTables, defragLwmPct, Integer.MAX_VALUE);
        Iterable<SSTableReader> targetSSTables = !defragCandidates.isEmpty() ? defragCandidates : filteredSSTables;

        ILifecycleTransaction txn = cfs.getTracker().tryModify(targetSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;

        return Collections.singletonList(new HMABlockDefragTask(cfs, txn, gcBefore, false));
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final long gcBefore)
    {
        assert !sstables.isEmpty();
        ILifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
            return null;

        return new HMABlockDefragTask(cfs, transaction, gcBefore, false).setUserDefined(true);
    }

    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        ArrayList<ISSTableScanner> scanners = new ArrayList<>(sstables.size());
        try
        {
            for (SSTableReader sstable : sstables)
            {
                ISSTableScanner rawScanner = sstable.getScanner(ranges, DatabaseDescriptor.getCompactionReadDiskAccessMode());
                scanners.add(HMABlockDefragTask.maybeWrapScanner(cfs, sstable, rawScanner));
            }
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }
        return new ScannerList(scanners);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);

        String lwmStr = options.get(DEFRAG_LWM_PCT_OPTION);
        if (lwmStr != null)
        {
            try
            {
                int lwm = Integer.parseInt(lwmStr);
                if (lwm < 1 || lwm > 99)
                    throw new ConfigurationException(DEFRAG_LWM_PCT_OPTION + " must be between 1 and 99");
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(DEFRAG_LWM_PCT_OPTION + " must be an integer", e);
            }
            uncheckedOptions.remove(DEFRAG_LWM_PCT_OPTION);
        }

        String maxBatchStr = options.get(MAX_DEFRAG_SSTABLES_OPTION);
        if (maxBatchStr != null)
        {
            try
            {
                int maxBatch = Integer.parseInt(maxBatchStr);
                if (maxBatch < 1)
                    throw new ConfigurationException(MAX_DEFRAG_SSTABLES_OPTION + " must be >= 1");
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(MAX_DEFRAG_SSTABLES_OPTION + " must be an integer", e);
            }
            uncheckedOptions.remove(MAX_DEFRAG_SSTABLES_OPTION);
        }

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
        return uncheckedOptions;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    @Override
    protected synchronized Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    @Override
    public String toString()
    {
        return String.format("HMABlockCompactionStrategy[lwm=%d%%, maxBatch=%d]", defragLwmPct, maxDefragSSTables);
    }
}
