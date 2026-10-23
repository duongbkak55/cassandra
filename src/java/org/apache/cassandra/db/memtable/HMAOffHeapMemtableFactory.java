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

package org.apache.cassandra.db.memtable;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.io.sstable.format.hma.HMAOffHeapArenaPool;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * Factory for {@link HMAOffHeapMemtable} (Part 5: Native Off-Heap Arena Memory Management).
 */
public class HMAOffHeapMemtableFactory implements Memtable.Factory
{
    public static final String CONFIGURATION_KEY = "hma_offheap";
    public static final String SLAB_SIZE_KB_OPTION = "slab_size_kb";
    public static final String AUTO_ENABLE_PROP = "cassandra.hma.memtable.auto_enable";

    public static final HMAOffHeapMemtableFactory INSTANCE = new HMAOffHeapMemtableFactory(HMAOffHeapArenaPool.DEFAULT_SLAB_SIZE_BYTES);
    public static final InheritingClass CONFIGURATION =
        new InheritingClass(null, HMAOffHeapMemtable.class.getName(), ImmutableMap.of());

    private static volatile boolean autoEnableForUserTables =
        Boolean.parseBoolean(System.getProperty(AUTO_ENABLE_PROP, "true"));

    private final int slabSizeBytes;

    public HMAOffHeapMemtableFactory(int slabSizeBytes)
    {
        this.slabSizeBytes = Math.max(64 * 1024, slabSizeBytes);
    }

    @Override
    public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                           TableMetadataRef metadataRef,
                           Memtable.Owner owner)
    {
        return new HMAOffHeapMemtable(commitLogLowerBound, metadataRef, owner, slabSizeBytes);
    }

    public static Memtable.Factory factory(Map<String, String> options)
    {
        String slabKb = options.remove(SLAB_SIZE_KB_OPTION);
        if (slabKb == null || slabKb.isEmpty())
            return INSTANCE;
        int kb = Integer.parseInt(slabKb);
        return new HMAOffHeapMemtableFactory(kb * 1024);
    }

    public static boolean isAutoEnableForUserTables()
    {
        return autoEnableForUserTables;
    }

    public static void setAutoEnableForUserTables(boolean enabled)
    {
        autoEnableForUserTables = enabled;
    }

    public static boolean isEligibleUserTable(TableMetadata metadata)
    {
        if (metadata == null)
            return false;
        if (SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return false;
        if (metadata.isIndex() || metadata.isView() || metadata.isVirtual() || metadata.isCounter())
            return false;
        return true;
    }

    public static Memtable.Factory resolveFactory(TableMetadata metadata)
    {
        if (metadata == null)
            return SkipListMemtableFactory.INSTANCE;
        Memtable.Factory configured = metadata.params.memtable.factory();
        if (configured == SkipListMemtableFactory.INSTANCE
            && metadata.params.memtable == MemtableParams.DEFAULT
            && autoEnableForUserTables
            && HMAOffHeapArenaPool.instance().isEnabled()
            && isEligibleUserTable(metadata))
        {
            return INSTANCE;
        }
        return configured;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof HMAOffHeapMemtableFactory))
            return false;
        HMAOffHeapMemtableFactory that = (HMAOffHeapMemtableFactory) o;
        return slabSizeBytes == that.slabSizeBytes;
    }

    @Override
    public int hashCode()
    {
        return Integer.hashCode(slabSizeBytes);
    }
}
