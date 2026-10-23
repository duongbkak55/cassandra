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

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.metrics.CASClientWriteRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Aerospike-inspired Partition-Level Fast-CAS Engine for Cassandra HMA (Phase 6.1).
 * <p>
 * Standard Cassandra LWT ({@code UPDATE ... IF ...}) executes a 4-phase Paxos protocol
 * (Prepare -> Read -> Propose -> Commit) that persists intermediate state to {@code system.paxos}
 * three times per conditional update, limiting single-node CAS throughput to ~7.9k TPS.
 * <p>
 * Like Aerospike's single-hop record lock on the 64-byte Primary Index entry, {@link HMAFastCasEngine}
 * serializes conditional Read-Modify-Write operations per partition in memory using a 16,384-stripe
 * lock table, evaluates CQL preconditions directly against the local HMA storage engine
 * ({@link org.apache.cassandra.db.memtable.HMAOffHeapMemtable} + {@link HMAPrimaryIndex}),
 * assigns a strictly monotonic microsecond timestamp, and applies the mutation in a single write pass
 * without touching {@code system.paxos}.
 * <p>
 * Completely transparent to CQL clients and the native protocol.
 */
public final class HMAFastCasEngine
{
    private static final Logger logger = LoggerFactory.getLogger(HMAFastCasEngine.class);

    public static final String FAST_CAS_ENABLED_PROP = "cassandra.hma.fast_cas.enabled";
    private static volatile boolean enabled = Boolean.parseBoolean(System.getProperty(FAST_CAS_ENABLED_PROP, "true"));

    /**
     * 16,384 lock stripes (power of 2) to minimize false contention across concurrent worker threads.
     * Pre-image cache is bounded to 4,096 slots (~20 MB max for 5 KiB rows).
     */
    private static final int STRIPE_COUNT = 16384;
    private static final int STRIPE_MASK = STRIPE_COUNT - 1;
    private static final int CACHE_SIZE = 8192;
    private static final int CACHE_MASK = CACHE_SIZE - 1;

    private static final ReentrantLock[] STRIPE_LOCKS = new ReentrantLock[STRIPE_COUNT];
    private static final long[] STRIPE_LAST_MICROS = new long[STRIPE_COUNT];
    private static final AtomicLongArray STRIPE_WRITE_SEQ = new AtomicLongArray(STRIPE_COUNT);
    private static final AtomicReferenceArray<CachedPreImage> STRIPE_CACHE = new AtomicReferenceArray<>(CACHE_SIZE);
    private static final AtomicLong CACHE_EPOCH = new AtomicLong(1L);

    private static final AtomicLong fastCasSuccessCount = new AtomicLong();
    private static final AtomicLong fastCasConditionNotMetCount = new AtomicLong();

    private static final class CachedPreImage
    {
        final long epoch;
        final TableId tableId;
        final DecoratedKey key;
        final FilteredPartition partition;
        final long cachedAtSec;

        CachedPreImage(long epoch, TableId tableId, DecoratedKey key, FilteredPartition partition, long cachedAtSec)
        {
            this.epoch = epoch;
            this.tableId = tableId;
            this.key = key;
            this.partition = partition;
            this.cachedAtSec = cachedAtSec;
        }
    }

    static
    {
        for (int i = 0; i < STRIPE_COUNT; i++)
        {
            STRIPE_LOCKS[i] = new ReentrantLock();
        }
        logger.info("Initialized HMAFastCasEngine with {} partition lock stripes (enabled={})", STRIPE_COUNT, enabled);
    }

    private HMAFastCasEngine()
    {
    }

    public static boolean isEnabled()
    {
        return enabled && HMAPrimaryIndexManager.instance().isEnabled();
    }

    public static void setEnabled(boolean flag)
    {
        enabled = flag;
        if (!flag)
            bumpCacheEpoch();
    }

    public static long getFastCasSuccessCount()
    {
        return fastCasSuccessCount.get();
    }

    public static long getFastCasConditionNotMetCount()
    {
        return fastCasConditionNotMetCount.get();
    }

    public static void bumpCacheEpoch()
    {
        CACHE_EPOCH.incrementAndGet();
        for (int i = 0; i < CACHE_SIZE; i++)
            STRIPE_CACHE.lazySet(i, null);
    }

    public static long currentCacheEpoch()
    {
        return CACHE_EPOCH.get();
    }

    public static int beginPartitionWrite(TableId tableId, DecoratedKey key)
    {
        int stripe = stripeFor(tableId, key);
        STRIPE_WRITE_SEQ.incrementAndGet(stripe);
        STRIPE_CACHE.lazySet(stripe & CACHE_MASK, null);
        return stripe;
    }

    public static void endPartitionWrite(int stripe)
    {
        STRIPE_WRITE_SEQ.incrementAndGet(stripe);
    }

    public static long readStripeSeq(TableId tableId, DecoratedKey key)
    {
        return STRIPE_WRITE_SEQ.get(stripeFor(tableId, key));
    }

    public static void recordPreRead(TableMetadata metadata,
                                     DecoratedKey key,
                                     FilteredPartition partition,
                                     long nowInSec,
                                     long seqBefore,
                                     long epochBefore)
    {
        if (!isEnabled() || metadata == null || partition == null || partition.isEmpty())
            return;
        if (metadata.isCounter() || SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return;

        int stripe = stripeFor(metadata.id, key);
        if ((seqBefore & 1L) != 0L)
            return;
        if (STRIPE_WRITE_SEQ.get(stripe) != seqBefore || CACHE_EPOCH.get() != epochBefore)
            return;

        int cacheSlot = stripe & CACHE_MASK;
        CachedPreImage entry = new CachedPreImage(epochBefore, metadata.id, key, partition, nowInSec);
        STRIPE_CACHE.lazySet(cacheSlot, entry);
        if (STRIPE_WRITE_SEQ.get(stripe) != seqBefore || CACHE_EPOCH.get() != epochBefore)
            STRIPE_CACHE.compareAndSet(cacheSlot, entry, null);
    }

    /**
     * Determines whether a CAS operation on the given table and key can take the HMA Fast-CAS path.
     */
    public static boolean canExecuteFastCas(TableMetadata metadata,
                                            DecoratedKey key,
                                            ConsistencyLevel consistencyForPaxos,
                                            ConsistencyLevel consistencyForCommit)
    {
        if (!isEnabled() || metadata == null || SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return false;

        Keyspace ks = Keyspace.open(metadata.keyspace);
        if (ks == null)
            return false;

        int rf = ks.getReplicationStrategy().getReplicationFactor().allReplicas;
        return rf <= 1;
    }

    private static int stripeFor(TableId tableId, DecoratedKey key)
    {
        long h = key.getToken().getLongValue() ^ tableId.asUUID().getLeastSignificantBits();
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        return ((int) h) & STRIPE_MASK;
    }

    /**
     * Computes the maximum timestamp present in the read partition so the conditional update
     * is guaranteed to supersede all existing cells and row markers.
     */
    private static long maxTimestampInPartition(FilteredPartition partition)
    {
        if (partition == null || partition.isEmpty())
            return Long.MIN_VALUE;

        long maxTs = Long.MIN_VALUE;

        Row staticRow = partition.staticRow();
        if (staticRow != null && !staticRow.isEmpty())
            maxTs = Math.max(maxTs, maxTimestampInRow(staticRow));

        for (Row row : partition)
        {
            maxTs = Math.max(maxTs, maxTimestampInRow(row));
        }
        return maxTs;
    }

    private static long maxTimestampInRow(Row row)
    {
        long maxTs = Long.MIN_VALUE;
        if (!row.primaryKeyLivenessInfo().isEmpty())
            maxTs = Math.max(maxTs, row.primaryKeyLivenessInfo().timestamp());
        if (!row.deletion().isLive())
            maxTs = Math.max(maxTs, row.deletion().time().markedForDeleteAt());

        for (ColumnData cd : row)
        {
            if (cd instanceof Cell<?>)
            {
                maxTs = Math.max(maxTs, ((Cell<?>) cd).timestamp());
            }
            else if (cd instanceof ComplexColumnData)
            {
                ComplexColumnData ccd = (ComplexColumnData) cd;
                if (!ccd.complexDeletion().isLive())
                    maxTs = Math.max(maxTs, ccd.complexDeletion().markedForDeleteAt());
                for (Cell<?> cell : ccd)
                    maxTs = Math.max(maxTs, cell.timestamp());
            }
        }
        return maxTs;
    }

    /**
     * Executes a single-partition CAS operation atomically under a partition stripe lock,
     * bypassing {@code system.paxos}.
     *
     * @return {@code null} if the condition was met and the update was applied,
     *         or a {@link RowIterator} of current values if the condition was not met.
     */
    public static RowIterator executeCas(TableMetadata metadata,
                                         DecoratedKey key,
                                         CASRequest request,
                                         ConsistencyLevel consistencyForPaxos,
                                         ConsistencyLevel consistencyForCommit,
                                         ClientState clientState,
                                         long nowInSeconds,
                                         Dispatcher.RequestTime requestTime,
                                         CASClientWriteRequestMetrics casWriteMetrics)
    throws InvalidRequestException
    {
        final int stripe = stripeFor(metadata.id, key);
        final ReentrantLock lock = STRIPE_LOCKS[stripe];
        lock.lock();
        try
        {
            // 1. Read current partition state from stripe pre-read cache or local HMA storage engine
            SinglePartitionReadCommand readCommand = request.readCommand(nowInSeconds);
            final boolean fetchesAllRegulars = fetchesAllRegularColumns(readCommand, metadata);
            FilteredPartition current = null;
            final int cacheSlot = stripe & CACHE_MASK;
            CachedPreImage cached = STRIPE_CACHE.get(cacheSlot);
            if (cached != null
                && cached.epoch == CACHE_EPOCH.get()
                && (STRIPE_WRITE_SEQ.get(stripe) & 1L) == 0L
                && cached.tableId.equals(metadata.id)
                && Math.abs(nowInSeconds - cached.cachedAtSec) <= 1L
                && fetchesAllRegulars
                && readCommand.clusteringIndexFilter().selectsAllPartition()
                && cached.key.equals(key))
            {
                current = cached.partition;
            }

            if (current == null)
            {
                try (ReadExecutionController executionController = readCommand.executionController();
                     PartitionIterator iter = readCommand.executeInternal(executionController))
                {
                    current = FilteredPartition.create(PartitionIterators.getOnlyElement(iter, readCommand));
                }
            }

            // 2. Evaluate CQL CAS precondition in memory
            if (!request.appliesTo(current))
            {
                fastCasConditionNotMetCount.incrementAndGet();
                if (casWriteMetrics != null)
                    casWriteMetrics.conditionNotMet.inc();
                return current.rowIterator(false);
            }

            // 3. Assign strictly monotonic microsecond timestamp for this partition stripe
            long maxExistingTs = maxTimestampInPartition(current);
            long wallMicros = FBUtilities.timestampMicros();
            long minRequiredTs = Math.max(wallMicros, maxExistingTs == Long.MIN_VALUE ? wallMicros : maxExistingTs + 1);
            long commitMicros = Math.max(minRequiredTs, STRIPE_LAST_MICROS[stripe] + 1);
            STRIPE_LAST_MICROS[stripe] = commitMicros;

            // Keep node-level Paxos/ClientState timestamp aware of our monotonic advance
            ClientState.getTimestampForPaxos(commitMicros);

            Ballot ballot = Ballot.atUnixMicrosWithLsb(commitMicros,
                                                       ThreadLocalRandom.current().nextLong(),
                                                       Ballot.Flag.LOCAL);

            // 4. Build PartitionUpdate directly with commitMicros
            PartitionUpdate updates;
            if (request instanceof CQL3CasRequest)
            {
                updates = ((CQL3CasRequest) request).makeUpdatesWithTimestamp(current, clientState, ballot, commitMicros);
            }
            else
            {
                PartitionUpdate raw = request.makeUpdates(current, clientState, ballot);
                updates = Commit.Proposal.of(ballot, raw).update;
            }

            ClientRequestSizeMetrics.recordRowAndColumnCountMetrics(updates);
            if (casWriteMetrics != null)
                casWriteMetrics.mutationSize.update(updates.dataSize());

            updates = TriggerExecutor.instance.execute(updates);

            FilteredPartition nextCachedPartition = null;
            if (metadata.clusteringColumns().isEmpty()
                && metadata.staticColumns().isEmpty()
                && !current.isEmpty()
                && updates.partitionLevelDeletion().isLive()
                && updates.rowCount() == 1
                && fetchesAllRegulars)
            {
                Row currentRow = current.getAtIdx(0);
                Row deltaRow = updates.iterator().next();
                if (currentRow != null && deltaRow != null && deltaRow.deletion().isLive())
                {
                    Row mergedRow = Rows.merge(currentRow, deltaRow)
                                        .purge(DeletionPurger.PURGE_ALL, nowInSeconds, metadata.enforceStrictLiveness())
                                        .updateAllTimestamp(commitMicros);
                    if (mergedRow != null && !mergedRow.isEmpty())
                    {
                        RegularAndStaticColumns allCols = metadata.regularAndStaticColumns();
                        EncodingStats mergedStats = current.stats().mergeWith(updates.stats());
                        DeletionTime overwriteBefore = DeletionTime.build(commitMicros - 1, nowInSeconds);
                        PartitionUpdate.Builder fullBuilder = new PartitionUpdate.Builder(metadata,
                                                                                          key,
                                                                                          allCols,
                                                                                          1,
                                                                                          false);
                        fullBuilder.addPartitionDeletion(overwriteBefore);
                        fullBuilder.add(mergedRow);
                        updates = fullBuilder.build();

                        nextCachedPartition = FilteredPartition.fromSingleRow(metadata,
                                                                              key,
                                                                              allCols,
                                                                              mergedRow,
                                                                              mergedStats);
                    }
                }
            }

            Mutation mutation = new Mutation(updates);

            // 5. Apply mutation directly (flows through HMAOffHeapMemtable + HMAStreamingWriteBuffer + HMAPrimaryIndex)
            Keyspace ks = Keyspace.open(metadata.keyspace);
            if (ks.getReplicationStrategy().getReplicationFactor().allReplicas <= 1)
            {
                mutation.apply();
            }
            else
            {
                StorageProxy.mutateWithTriggers(Collections.singletonList(mutation),
                                                consistencyForCommit,
                                                false,
                                                requestTime,
                                                PreserveTimestamp.yes);
            }

            if (nextCachedPartition != null && (STRIPE_WRITE_SEQ.get(stripe) & 1L) == 0L)
            {
                STRIPE_CACHE.lazySet(cacheSlot,
                                     new CachedPreImage(CACHE_EPOCH.get(),
                                                        metadata.id,
                                                        key,
                                                        nextCachedPartition,
                                                        nowInSeconds));
            }

            fastCasSuccessCount.incrementAndGet();
            return null;
        }
        finally
        {
            lock.unlock();
        }
    }

    private static boolean fetchesAllRegularColumns(SinglePartitionReadCommand readCommand, TableMetadata metadata)
    {
        return readCommand.columnFilter().fetchesAllColumns(false)
               || readCommand.columnFilter().fetchedColumns().regulars.equals(metadata.regularAndStaticColumns().regulars);
    }
}
