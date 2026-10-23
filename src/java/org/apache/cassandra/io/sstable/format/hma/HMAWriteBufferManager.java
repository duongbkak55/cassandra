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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Throwables;

/**
 * Aerospike-inspired Single-Write Streaming Write Buffer Manager (Part 4).
 *
 * <p>Eliminates the traditional Cassandra Double-Write (CommitLog append + Memtable flush) by:
 * <ol>
 *   <li>Skipping {@code CommitLog.instance.add(mutation)} for tables running in Single-Write mode.</li>
 *   <li>Appending mutations directly into a per-table 4KB-aligned off-heap {@link HMAStreamingWriteBuffer}
 *       (default 1 MiB block size) while bypassing {@code Memtable.put()}.</li>
 *   <li>Flushing the Streaming Write Buffer directly to an SSTable in a single sequential pass as soon as
 *       the buffer reaches capacity (or on explicit flush), immediately indexing all keys in the 64-byte
 *       off-heap {@link HMAPrimaryIndex}.</li>
 * </ol>
 */
public final class HMAWriteBufferManager
{
    private static final Logger logger = LoggerFactory.getLogger(HMAWriteBufferManager.class);

    public static final String ENABLE_SINGLE_WRITE_PROP = "cassandra.hma.single_write.enabled";
    public static final String SWB_BLOCK_SIZE_PROP = "cassandra.hma.swb.block_size_bytes";
    public static final int DEFAULT_SWB_BLOCK_SIZE_BYTES = 1024 * 1024; // 1 MiB (Aerospike write-block size)

    private static final HMAWriteBufferManager INSTANCE = new HMAWriteBufferManager();

    private final AtomicBoolean singleWriteEnabled = new AtomicBoolean(
        Boolean.parseBoolean(System.getProperty(ENABLE_SINGLE_WRITE_PROP, "false"))
    );
    private final AtomicInteger blockSizeBytes = new AtomicInteger(
        Integer.getInteger(SWB_BLOCK_SIZE_PROP, DEFAULT_SWB_BLOCK_SIZE_BYTES)
    );
    private final Set<TableId> enabledTables = ConcurrentHashMap.newKeySet();
    private final AtomicLong bufferIdSeq = new AtomicLong(1);

    private final ConcurrentHashMap<TableId, HMAStreamingWriteBuffer> activeBuffers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TableId, CopyOnWriteArrayList<HMAStreamingWriteBuffer>> flushingBuffers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TableId, CopyOnWriteArrayList<CompletableFuture<Collection<SSTableReader>>>> pendingFlushFutures = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TableId, Object> tableLocks = new ConcurrentHashMap<>();
    private final ExecutorService swbFlushExecutor;

    private final LongAdder commitLogBypassedCount = new LongAdder();
    private final LongAdder memtableBypassedCount = new LongAdder();
    private final LongAdder swbMutationsAppended = new LongAdder();
    private final LongAdder swbBytesBuffered = new LongAdder();
    private final LongAdder swbBlocksFlushed = new LongAdder();
    private final LongAdder swbSSTablesWritten = new LongAdder();

    private HMAWriteBufferManager()
    {
        AtomicInteger threadIdx = new AtomicInteger(1);
        this.swbFlushExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "HMA-SWB-Flusher-" + threadIdx.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
    }

    public static HMAWriteBufferManager instance()
    {
        return INSTANCE;
    }

    public boolean isSingleWriteEnabled()
    {
        return singleWriteEnabled.get();
    }

    public void setSingleWriteEnabled(boolean enabled)
    {
        singleWriteEnabled.set(enabled);
    }

    public void enableSingleWriteForTable(TableId tableId)
    {
        if (tableId != null)
            enabledTables.add(tableId);
    }

    public void disableSingleWriteForTable(TableId tableId)
    {
        if (tableId != null)
            enabledTables.remove(tableId);
    }

    public int getBlockSizeBytes()
    {
        return blockSizeBytes.get();
    }

    public void setBlockSizeBytes(int bytes)
    {
        blockSizeBytes.set(Math.max(4096, bytes));
    }

    /**
     * Checks whether the given table is eligible for Single-Write Streaming Write Buffer mode.
     */
    public boolean isSingleWriteEnabled(TableMetadata metadata)
    {
        if (metadata == null || metadata.isIndex() || metadata.isView())
            return false;
        if (SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return false;
        return singleWriteEnabled.get() || enabledTables.contains(metadata.id);
    }

    /**
     * Returns {@code true} if all partition updates in {@code mutation} target tables with Single-Write enabled,
     * allowing {@code CassandraKeyspaceWriteHandler} to bypass {@code CommitLog.instance.add(mutation)}.
     */
    public boolean shouldBypassCommitLog(Mutation mutation)
    {
        if (mutation == null)
            return false;
        Collection<PartitionUpdate> updates = mutation.getPartitionUpdates();
        if (updates.isEmpty())
            return false;

        for (PartitionUpdate update : updates)
        {
            if (!isSingleWriteEnabled(update.metadata()))
                return false;
        }
        commitLogBypassedCount.increment();
        return true;
    }

    /**
     * Appends {@code update} into the table's active {@link HMAStreamingWriteBuffer}, bypassing {@code Memtable}.
     * Automatically flushes the streaming block directly to an SSTable if the buffer reaches {@link #getBlockSizeBytes()}.
     */
    public void write(ColumnFamilyStore cfs, PartitionUpdate update)
    {
        TableId tableId = cfs.metadata.id;
        HMAStreamingWriteBuffer active = activeBuffers.get(tableId);
        if (active != null && !active.isFull() && active.append(update))
        {
            memtableBypassedCount.increment();
            swbMutationsAppended.increment();
            swbBytesBuffered.add(Math.max(64, update.dataSize()));
            if (!active.isFull())
                return;

            Object lock = tableLocks.computeIfAbsent(tableId, id -> new Object());
            HMAStreamingWriteBuffer toFlush = null;
            synchronized (lock)
            {
                if (active.isFull() && active.seal())
                {
                    flushingBuffers.computeIfAbsent(tableId, id -> new CopyOnWriteArrayList<>()).add(active);
                    activeBuffers.remove(tableId, active);
                    toFlush = active;
                }
            }
            if (toFlush != null)
                scheduleAsyncFlush(cfs, toFlush);
            return;
        }

        Object lock = tableLocks.computeIfAbsent(tableId, id -> new Object());
        HMAStreamingWriteBuffer toFlush = null;

        synchronized (lock)
        {
            while (true)
            {
                active = activeBuffers.computeIfAbsent(
                    tableId,
                    id -> new HMAStreamingWriteBuffer(bufferIdSeq.getAndIncrement(), cfs.metadata(), blockSizeBytes.get())
                );
                if (active.append(update))
                {
                    memtableBypassedCount.increment();
                    swbMutationsAppended.increment();
                    swbBytesBuffered.add(Math.max(64, update.dataSize()));
                    if (active.isFull() && active.seal())
                    {
                        flushingBuffers.computeIfAbsent(tableId, id -> new CopyOnWriteArrayList<>()).add(active);
                        activeBuffers.remove(tableId, active);
                        toFlush = active;
                    }
                    break;
                }
                else
                {
                    activeBuffers.remove(tableId, active);
                }
            }
        }

        if (toFlush != null)
        {
            scheduleAsyncFlush(cfs, toFlush);
        }
    }

    private void scheduleAsyncFlush(ColumnFamilyStore cfs, HMAStreamingWriteBuffer toFlush)
    {
        TableId tableId = cfs.metadata.id;
        CopyOnWriteArrayList<CompletableFuture<Collection<SSTableReader>>> futures =
            pendingFlushFutures.computeIfAbsent(tableId, id -> new CopyOnWriteArrayList<>());
        CompletableFuture<Collection<SSTableReader>> future =
            CompletableFuture.supplyAsync(() -> flushSealedBuffer(cfs, toFlush), swbFlushExecutor);
        futures.add(future);
        future.whenComplete((res, err) -> futures.remove(future));
    }

    /**
     * Flushes any active non-empty {@link HMAStreamingWriteBuffer} for {@code cfs} directly to an SSTable.
     */
    public Collection<SSTableReader> flushTable(ColumnFamilyStore cfs)
    {
        if (cfs == null)
            return Collections.emptyList();

        TableId tableId = cfs.metadata.id;
        List<SSTableReader> allFlushed = new ArrayList<>();
        CopyOnWriteArrayList<CompletableFuture<Collection<SSTableReader>>> inFlightFutures = pendingFlushFutures.get(tableId);
        if (inFlightFutures != null && !inFlightFutures.isEmpty())
        {
            for (CompletableFuture<Collection<SSTableReader>> f : new ArrayList<>(inFlightFutures))
            {
                try
                {
                    Collection<SSTableReader> res = f.join();
                    if (res != null)
                        allFlushed.addAll(res);
                }
                catch (Exception ignored)
                {
                }
            }
        }

        Object lock = tableLocks.computeIfAbsent(tableId, id -> new Object());
        HMAStreamingWriteBuffer toFlush = null;

        synchronized (lock)
        {
            HMAStreamingWriteBuffer active = activeBuffers.get(tableId);
            if (active != null && !active.isEmpty() && active.seal())
            {
                flushingBuffers.computeIfAbsent(tableId, id -> new CopyOnWriteArrayList<>()).add(active);
                activeBuffers.remove(tableId, active);
                toFlush = active;
            }
            else if (active != null && active.isEmpty())
            {
                activeBuffers.remove(tableId, active);
                active.release();
            }
        }

        if (toFlush != null)
            allFlushed.addAll(flushSealedBuffer(cfs, toFlush));

        return allFlushed;
    }

    private Collection<SSTableReader> flushSealedBuffer(ColumnFamilyStore cfs, HMAStreamingWriteBuffer buffer)
    {
        TableId tableId = cfs.metadata.id;
        try
        {
            Collection<PartitionUpdate> sortedPartitions = buffer.getSortedPartitions();
            if (sortedPartitions.isEmpty())
                return Collections.emptyList();

            RegularAndStaticColumns.Builder columnsBuilder = RegularAndStaticColumns.builder();
            EncodingStats stats = EncodingStats.NO_STATS;
            for (PartitionUpdate update : sortedPartitions)
            {
                columnsBuilder.addAll(update.columns());
                stats = stats.mergeWith(update.stats());
            }

            SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
            Descriptor descriptor = cfs.newSSTableDescriptor(
                cfs.getDirectories().getWriteableLocationAsFile(Math.max(4096L, buffer.getBufferedBytes())),
                format
            );

            List<SSTableReader> sstables = new ArrayList<>();
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.FLUSH))
            {
                SerializationHeader header = new SerializationHeader(true,
                                                                     cfs.metadata(),
                                                                     columnsBuilder.build(),
                                                                     stats,
                                                                     false);
                SSTableMultiWriter writer = cfs.createSSTableMultiWriter(descriptor,
                                                                         sortedPartitions.size(),
                                                                         ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                         ActiveRepairService.NO_PENDING_REPAIR,
                                                                         false,
                                                                         IntervalSet.empty(),
                                                                         header,
                                                                         txn);
                try
                {
                    for (PartitionUpdate update : sortedPartitions)
                    {
                        if (!update.isEmpty())
                        {
                            try (UnfilteredRowIterator iter = update.unfilteredIterator())
                            {
                                writer.append(iter);
                            }
                        }
                    }

                    if (writer.getBytesWritten() > 0)
                    {
                        writer.setOpenResult(true).prepareToCommit();
                        txn.prepareToCommit();
                        Throwable accumulate = writer.commit(null);
                        cfs.metric.flushSizeOnDisk.update(writer.getOnDiskBytesWritten());
                        cfs.metric.bytesFlushed.inc(writer.getBytesWritten());
                        Throwables.maybeFail(txn.commit(accumulate));
                        for (SSTableReader sstable : writer.finished())
                        {
                            if (sstable != null)
                                sstables.add(sstable);
                        }
                    }
                    else
                    {
                        Throwables.maybeFail(writer.abort(null));
                        Throwables.maybeFail(txn.abort(null));
                    }
                }
                catch (Throwable t)
                {
                    t = writer.abort(t);
                    t = txn.abort(t);
                    Throwables.maybeFail(t);
                    throw new RuntimeException(t);
                }
            }

            if (!sstables.isEmpty())
            {
                cfs.getTracker().addSSTables(sstables);
                swbBlocksFlushed.increment();
                swbSSTablesWritten.add(sstables.size());
                logger.info("HMA Single-Write Buffer #{} flushed directly to {} SSTables ({} partitions, {} bytes) for {}.{}",
                            buffer.getBufferId(), sstables.size(), sortedPartitions.size(),
                            buffer.getBufferedBytes(), cfs.getKeyspaceName(), cfs.getTableName());
            }
            return sstables;
        }
        finally
        {
            CopyOnWriteArrayList<HMAStreamingWriteBuffer> inFlight = flushingBuffers.get(tableId);
            if (inFlight != null)
                inFlight.remove(buffer);
            buffer.release();
        }
    }

    /**
     * Collects {@link UnfilteredRowIterator}s from any in-flight or active {@link HMAStreamingWriteBuffer}s
     * for point/slice reads arriving before the 1 MiB block has been flushed to disk.
     */
    public void collectRowIterators(ColumnFamilyStore cfs,
                                    DecoratedKey key,
                                    ClusteringIndexFilter filter,
                                    ColumnFilter columnFilter,
                                    List<UnfilteredRowIterator> target)
    {
        if (cfs == null)
            return;
        TableId tableId = cfs.metadata.id;

        CopyOnWriteArrayList<HMAStreamingWriteBuffer> inFlight = flushingBuffers.get(tableId);
        if (inFlight != null)
        {
            for (HMAStreamingWriteBuffer buf : inFlight)
            {
                UnfilteredRowIterator iter = buf.rowIterator(key, filter, columnFilter);
                if (iter != null)
                    target.add(iter);
            }
        }

        HMAStreamingWriteBuffer active = activeBuffers.get(tableId);
        if (active != null)
        {
            UnfilteredRowIterator iter = active.rowIterator(key, filter, columnFilter);
            if (iter != null)
                target.add(iter);
        }
    }

    /**
     * Clears and releases any buffered data on table truncate/drop.
     */
    public void clearTable(TableId tableId)
    {
        if (tableId == null)
            return;
        HMAStreamingWriteBuffer active = activeBuffers.remove(tableId);
        if (active != null)
            active.release();
        CopyOnWriteArrayList<HMAStreamingWriteBuffer> inFlight = flushingBuffers.remove(tableId);
        if (inFlight != null)
        {
            for (HMAStreamingWriteBuffer buf : inFlight)
                buf.release();
            inFlight.clear();
        }
    }

    public long getCommitLogBypassedCount()
    {
        return commitLogBypassedCount.sum();
    }

    public long getMemtableBypassedCount()
    {
        return memtableBypassedCount.sum();
    }

    public long getSwbMutationsAppended()
    {
        return swbMutationsAppended.sum();
    }

    public long getSwbBytesBuffered()
    {
        return swbBytesBuffered.sum();
    }

    public long getSwbBlocksFlushed()
    {
        return swbBlocksFlushed.sum();
    }

    public long getStreamingBufferFlushes()
    {
        return swbBlocksFlushed.sum();
    }

    public long getStreamingBufferBytesFlushed()
    {
        return swbBytesBuffered.sum();
    }

    public long getSwbSSTablesWritten()
    {
        return swbSSTablesWritten.sum();
    }

    public void resetMetrics()
    {
        commitLogBypassedCount.reset();
        memtableBypassedCount.reset();
        swbMutationsAppended.reset();
        swbBytesBuffered.reset();
        swbBlocksFlushed.reset();
        swbSSTablesWritten.reset();
    }
}
