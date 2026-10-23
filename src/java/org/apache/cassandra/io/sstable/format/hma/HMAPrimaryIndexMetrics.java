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

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMX metrics bridge for HMA Primary Index.
 * <p>
 * Registers under ObjectName: org.apache.cassandra.io.sstable:type=HMAPrimaryIndex
 */
public class HMAPrimaryIndexMetrics implements HMAPrimaryIndexMXBean
{
    private static final Logger logger = LoggerFactory.getLogger(HMAPrimaryIndexMetrics.class);
    private static final String MBEAN_NAME = "org.apache.cassandra.io.sstable:type=HMAPrimaryIndex";

    private static volatile HMAPrimaryIndexMetrics instance;

    private HMAPrimaryIndexMetrics()
    {
    }

    public static synchronized void register()
    {
        if (instance != null)
            return;

        instance = new HMAPrimaryIndexMetrics();
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(MBEAN_NAME);
            if (!mbs.isRegistered(name))
            {
                mbs.registerMBean(instance, name);
                logger.info("HMA: Registered JMX MBean {}", MBEAN_NAME);
            }
        }
        catch (Exception e)
        {
            logger.warn("HMA: Failed to register JMX MBean", e);
        }
    }

    @Override
    public int getIndexedTableCount()
    {
        return HMAPrimaryIndexManager.instance().indexedTableCount();
    }

    @Override
    public long getTotalIndexedEntries()
    {
        return HMAPrimaryIndexManager.instance().totalEntries();
    }

    @Override
    public long getTotalOffHeapMemoryBytes()
    {
        return HMAPrimaryIndexManager.instance().totalOffHeapMemory();
    }

    @Override
    public long getTotalOffHeapMemoryMB()
    {
        return getTotalOffHeapMemoryBytes() / (1024 * 1024);
    }

    @Override
    public double getAverageBytesPerEntry()
    {
        long entries = getTotalIndexedEntries();
        if (entries == 0)
            return 0.0;
        return (double) getTotalOffHeapMemoryBytes() / entries;
    }

    @Override
    public long getTotalLookups()
    {
        return HMAPrimaryIndexManager.instance().totalLookups();
    }

    @Override
    public long getTotalHits()
    {
        return HMAPrimaryIndexManager.instance().totalHits();
    }

    @Override
    public long getTotalMisses()
    {
        return HMAPrimaryIndexManager.instance().totalMisses();
    }

    @Override
    public double getHitRate()
    {
        long lookups = getTotalLookups();
        if (lookups == 0)
            return 0.0;
        return (double) getTotalHits() / lookups;
    }

    @Override
    public long getDirectReadBypassCount()
    {
        return HMAPrimaryIndexManager.instance().totalDirectReadBypasses();
    }

    @Override
    public long getTotalDirectFreedSSTables()
    {
        return HMAPrimaryIndexManager.instance().totalDirectFreedSSTables();
    }

    @Override
    public long getTotalDefragRuns()
    {
        return HMAPrimaryIndexManager.instance().totalDefragRuns();
    }

    @Override
    public long getTotalDefraggedSSTables()
    {
        return HMAPrimaryIndexManager.instance().totalDefraggedSSTables();
    }

    @Override
    public long getTotalDefragMovedRecords()
    {
        return HMAPrimaryIndexManager.instance().totalDefragMovedRecords();
    }

    @Override
    public long getTotalDefragSkippedRecords()
    {
        return HMAPrimaryIndexManager.instance().totalDefragSkippedRecords();
    }

    @Override
    public long getTotalDefragReclaimedBytes()
    {
        return HMAPrimaryIndexManager.instance().totalDefragReclaimedBytes();
    }

    @Override
    public long getTotalTrackedDiskBytes()
    {
        return HMAPrimaryIndexManager.instance().totalTrackedDiskBytes();
    }

    @Override
    public long getTotalTrackedLiveBytes()
    {
        return HMAPrimaryIndexManager.instance().totalTrackedLiveBytes();
    }

    @Override
    public double getGlobalLiveUtilizationPct()
    {
        long disk = getTotalTrackedDiskBytes();
        if (disk <= 0)
            return 100.0;
        return (getTotalTrackedLiveBytes() * 100.0) / disk;
    }

    @Override
    public boolean isDirectIOEnabled()
    {
        return HMADirectIOEngine.instance().isDirectIOEnabled();
    }

    @Override
    public void setDirectIOEnabled(boolean enabled)
    {
        HMADirectIOEngine.instance().setDirectIOEnabled(enabled);
    }

    @Override
    public long getDirectIOReads()
    {
        return HMADirectIOEngine.instance().getDirectIOReads();
    }

    @Override
    public long getDirectIOWrites()
    {
        return HMADirectIOEngine.instance().getDirectIOWrites();
    }

    @Override
    public long getDirectIOBytesRead()
    {
        return HMADirectIOEngine.instance().getDirectIOBytesRead();
    }

    @Override
    public long getDirectIOBytesWritten()
    {
        return HMADirectIOEngine.instance().getDirectIOBytesWritten();
    }

    @Override
    public long getAsyncIOSubmitted()
    {
        return HMADirectIOEngine.instance().getAsyncIOSubmitted();
    }

    @Override
    public long getAsyncIOCompleted()
    {
        return HMADirectIOEngine.instance().getAsyncIOCompleted();
    }

    @Override
    public boolean isSingleWriteEnabled()
    {
        return HMAWriteBufferManager.instance().isSingleWriteEnabled();
    }

    @Override
    public void setSingleWriteEnabled(boolean enabled)
    {
        HMAWriteBufferManager.instance().setSingleWriteEnabled(enabled);
    }

    @Override
    public long getStreamingBufferFlushes()
    {
        return HMAWriteBufferManager.instance().getStreamingBufferFlushes();
    }

    @Override
    public long getCommitLogBypassedCount()
    {
        return HMAWriteBufferManager.instance().getCommitLogBypassedCount();
    }

    @Override
    public long getMemtableBypassedCount()
    {
        return HMAWriteBufferManager.instance().getMemtableBypassedCount();
    }

    @Override
    public long getStreamingBufferBytesFlushed()
    {
        return HMAWriteBufferManager.instance().getStreamingBufferBytesFlushed();
    }

    @Override
    public boolean isOffHeapArenaAvailable()
    {
        return HMAOffHeapArenaPool.instance().isEnabled();
    }

    @Override
    public boolean isFfmArenaSupported()
    {
        return HMAOffHeapArenaPool.instance().isFfmArenaAvailable();
    }

    @Override
    public int getOffHeapArenaSlabSizeBytes()
    {
        return HMAOffHeapArenaPool.instance().slabSizeBytes();
    }

    @Override
    public int getOffHeapArenaPooledSlabs()
    {
        return HMAOffHeapArenaPool.instance().pooledSlabsCount();
    }

    @Override
    public int getOffHeapArenaActiveSlabs()
    {
        return HMAOffHeapArenaPool.instance().activeSlabsCount();
    }

    @Override
    public long getOffHeapArenaActiveBytes()
    {
        return HMAOffHeapArenaPool.instance().activeArenaBytes();
    }

    @Override
    public long getOffHeapArenaTotalBytesAllocated()
    {
        return HMAOffHeapArenaPool.instance().totalBytesAllocated();
    }

    @Override
    public long getOffHeapArenaSlabHits()
    {
        return HMAOffHeapArenaPool.instance().slabHits();
    }

    @Override
    public long getOffHeapArenaSlabMisses()
    {
        return HMAOffHeapArenaPool.instance().slabMisses();
    }

    @Override
    public long getOffHeapArenaCreatedCount()
    {
        return HMAOffHeapArenaPool.instance().arenasCreated();
    }

    @Override
    public long getOffHeapArenaClosedCount()
    {
        return HMAOffHeapArenaPool.instance().arenasClosed();
    }

    @Override
    public long getOffHeapMemtablePutOps()
    {
        return org.apache.cassandra.db.memtable.HMAOffHeapMemtable.getOffHeapPutOps();
    }

    @Override
    public long getOffHeapMemtableDirectLookupHits()
    {
        return org.apache.cassandra.db.memtable.HMAOffHeapMemtable.getDirectLookupHits();
    }

    @Override
    public long getOffHeapMemtableDirectLookupMisses()
    {
        return org.apache.cassandra.db.memtable.HMAOffHeapMemtable.getDirectLookupMisses();
    }

    @Override
    public boolean isFastCasEnabled()
    {
        return HMAFastCasEngine.isEnabled();
    }

    @Override
    public void setFastCasEnabled(boolean enabled)
    {
        HMAFastCasEngine.setEnabled(enabled);
    }

    @Override
    public long getFastCasSuccessCount()
    {
        return HMAFastCasEngine.getFastCasSuccessCount();
    }

    @Override
    public long getFastCasConditionNotMetCount()
    {
        return HMAFastCasEngine.getFastCasConditionNotMetCount();
    }
}
