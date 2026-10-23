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

import java.util.concurrent.locks.StampedLock;

import org.apache.cassandra.io.util.Memory;

/**
 * Single shard of the Off-Heap HMA Primary Index.
 *
 * <p>Each entry occupies exactly 64 bytes (aligned to CPU cache line).
 * Open-addressing hash table with linear probing and StampedLock concurrency.
 * Reads utilize optimistic locking for near-zero overhead on uncontended paths.
 */
public final class HMAPrimaryIndexShard implements AutoCloseable
{
    private static final int DEFAULT_INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.70f;
    private static final int MAX_PROBE_LENGTH = 128;

    private final StampedLock lock = new StampedLock();
    private Memory memory;
    private int capacity;
    private int mask;
    private int size;
    private int threshold;
    private volatile boolean isClosed = false;

    public HMAPrimaryIndexShard()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public HMAPrimaryIndexShard(int initialCapacity)
    {
        int cap = 1;
        while (cap < initialCapacity)
            cap <<= 1;

        this.capacity = cap;
        this.mask = cap - 1;
        this.size = 0;
        this.threshold = (int) (cap * LOAD_FACTOR);

        long bytes = (long) cap * HMARecordPointer.ENTRY_SIZE;
        this.memory = Memory.allocate(bytes);
        // Zero-fill initial memory
        this.memory.setMemory(0, bytes, (byte) 0);
    }

    public int size()
    {
        return size;
    }

    public int capacity()
    {
        return capacity;
    }

    public long allocatedBytes()
    {
        return (long) capacity * HMARecordPointer.ENTRY_SIZE;
    }

    /**
     * Look up a record pointer by its 128-bit key hash.
     * Uses optimistic read lock for sub-microsecond latency.
     *
     * @return true if record exists and is not a tombstone, false otherwise.
     */
    public boolean get(long hashHi, long hashLo, HMARecordPointer outPointer)
    {
        long stamp = lock.tryOptimisticRead();
        boolean found = false;

        if (stamp != 0L)
        {
            found = getInternal(hashHi, hashLo, outPointer);
            if (lock.validate(stamp))
                return found;
        }

        // Fallback to acquired read lock if optimistic read was invalidated
        stamp = lock.readLock();
        try
        {
            return getInternal(hashHi, hashLo, outPointer);
        }
        finally
        {
            lock.unlockRead(stamp);
        }
    }

    private boolean getInternal(long hashHi, long hashLo, HMARecordPointer out)
    {
        if (out != null)
            out.flags = HMARecordPointer.FLAG_EMPTY;

        if (isClosed || size == 0)
            return false;

        int idx = (int) (hashHi ^ (hashHi >>> 32) ^ hashLo) & mask;
        Memory mem = this.memory;

        for (int step = 0; step < capacity; step++)
        {
            long offset = (long) idx * HMARecordPointer.ENTRY_SIZE;
            byte flags = mem.getByte(offset + HMARecordPointer.OFFSET_FLAGS);

            if (flags == HMARecordPointer.FLAG_EMPTY)
            {
                // Slot is empty, key is not present in hash chain
                return false;
            }

            long hi = mem.getLong(offset + HMARecordPointer.OFFSET_KEY_HASH_HI);
            long lo = mem.getLong(offset + HMARecordPointer.OFFSET_KEY_HASH_LO);

            if (hi == hashHi && lo == hashLo)
            {
                if ((flags & HMARecordPointer.FLAG_TOMBSTONE) != 0)
                {
                    if (out != null)
                        out.flags = flags;
                    // Deleted tombstone
                    return false;
                }

                if (out != null)
                {
                    out.readFrom(mem, offset);
                }
                return true;
            }

            idx = (idx + 1) & mask;
        }

        return false;
    }

    /**
     * Insert or update a record pointer with Last-Write-Wins timestamp semantics.
     */
    public boolean put(long hashHi,
                       long hashLo,
                       long sstableId,
                       long dataOffset,
                       int dataLength,
                       long timestamp,
                       int generation,
                       byte flags)
    {
        return put(hashHi, hashLo, sstableId, dataOffset, dataLength, timestamp, generation, flags, null);
    }

    /**
     * Insert or update a record pointer with Last-Write-Wins timestamp semantics and block-level defrag accounting.
     */
    public boolean put(long hashHi,
                       long hashLo,
                       long sstableId,
                       long dataOffset,
                       int dataLength,
                       long timestamp,
                       int generation,
                       byte flags,
                       HMAPrimaryIndex owner)
    {
        long stamp = lock.writeLock();
        try
        {
            checkNotClosed();

            if (size >= threshold)
            {
                resize(capacity << 1);
            }

            int idx = (int) (hashHi ^ (hashHi >>> 32) ^ hashLo) & mask;
            Memory mem = this.memory;
            int firstAvailableSlot = -1;

            for (int step = 0; step < capacity; step++)
            {
                long offset = (long) idx * HMARecordPointer.ENTRY_SIZE;
                byte slotFlags = mem.getByte(offset + HMARecordPointer.OFFSET_FLAGS);

                if (slotFlags == HMARecordPointer.FLAG_EMPTY)
                {
                    if (firstAvailableSlot == -1)
                        firstAvailableSlot = idx;
                    break;
                }

                long hi = mem.getLong(offset + HMARecordPointer.OFFSET_KEY_HASH_HI);
                long lo = mem.getLong(offset + HMARecordPointer.OFFSET_KEY_HASH_LO);

                if (hi == hashHi && lo == hashLo)
                {
                    long existingSstableId = mem.getLong(offset + HMARecordPointer.OFFSET_SSTABLE_ID);
                    long existingDataOffset = mem.getLong(offset + HMARecordPointer.OFFSET_DATA_OFFSET);
                    int existingDataLength = mem.getInt(offset + HMARecordPointer.OFFSET_DATA_LENGTH);
                    int existingBaseSstableId = mem.getInt(offset + HMARecordPointer.OFFSET_BASE_SSTABLE_ID);
                    long existingBaseDataOffset = mem.getLong(offset + HMARecordPointer.OFFSET_BASE_DATA_OFFSET);
                    boolean incomingComplete = (flags & HMARecordPointer.FLAG_COMPLETE_OVERWRITE) != 0
                                               || (flags & HMARecordPointer.FLAG_TOMBSTONE) != 0;
                    boolean existingComplete = (slotFlags & HMARecordPointer.FLAG_COMPLETE_OVERWRITE) != 0;
                    boolean differentSstable = existingSstableId != 0L && sstableId != 0L && existingSstableId != sstableId;
                    boolean samePhysicalRecord = existingSstableId != 0L && existingSstableId == sstableId && existingDataOffset == dataOffset;

                    // Existing key found - check Last-Write-Wins timestamp
                    long existingTimestamp = mem.getLong(offset + HMARecordPointer.OFFSET_TIMESTAMP);
                    if (timestamp < existingTimestamp ||
                        (timestamp == existingTimestamp && existingSstableId > 0 && sstableId > 0 && sstableId < existingSstableId))
                    {
                        if (!existingComplete && differentSstable)
                        {
                            if ((slotFlags & HMARecordPointer.FLAG_MULTI_SSTABLE) == 0
                                && (slotFlags & HMARecordPointer.FLAG_MULTI_BLOCK) == 0
                                && (flags & HMARecordPointer.FLAG_MULTI_BLOCK) == 0
                                && sstableId > 0 && sstableId <= Integer.MAX_VALUE)
                            {
                                byte updatedFlags = (byte) (slotFlags | HMARecordPointer.FLAG_MULTI_SSTABLE | HMARecordPointer.FLAG_TWO_SSTABLE);
                                if (incomingComplete)
                                    updatedFlags |= HMARecordPointer.FLAG_BASE_COMPLETE;
                                mem.setInt(offset + HMARecordPointer.OFFSET_BASE_SSTABLE_ID, (int) sstableId);
                                mem.setLong(offset + HMARecordPointer.OFFSET_BASE_DATA_OFFSET, dataOffset);
                                mem.setByte(offset + HMARecordPointer.OFFSET_FLAGS, updatedFlags);
                            }
                            else if ((slotFlags & HMARecordPointer.FLAG_BASE_COMPLETE) == 0
                                     || existingBaseSstableId <= 0
                                     || sstableId > existingBaseSstableId)
                            {
                                mem.setByte(offset + HMARecordPointer.OFFSET_FLAGS,
                                            (byte) ((slotFlags | HMARecordPointer.FLAG_MULTI_SSTABLE) & ~HMARecordPointer.FLAG_TWO_SSTABLE));
                            }
                        }
                        if (owner != null && sstableId != 0L && !samePhysicalRecord && (flags & HMARecordPointer.FLAG_TOMBSTONE) == 0)
                        {
                            owner.onRecordWritten(sstableId, dataOffset, dataLength, false);
                        }
                        // Stale update, ignore
                        return false;
                    }

                    boolean isDefragOrCompactionMove = (timestamp == existingTimestamp && differentSstable);
                    if (isDefragOrCompactionMove && existingComplete)
                    {
                        incomingComplete = true;
                        flags |= HMARecordPointer.FLAG_COMPLETE_OVERWRITE;
                    }

                    byte mergedFlags = flags;
                    int newBaseSstableId = 0;
                    long newBaseDataOffset = -1L;

                    if (incomingComplete)
                    {
                        mergedFlags = (byte) ((flags | HMARecordPointer.FLAG_COMPLETE_OVERWRITE)
                                              & ~(HMARecordPointer.FLAG_MULTI_SSTABLE
                                                  | HMARecordPointer.FLAG_TWO_SSTABLE
                                                  | HMARecordPointer.FLAG_BASE_COMPLETE));
                    }
                    else if (!isDefragOrCompactionMove)
                    {
                        if (!differentSstable)
                        {
                            mergedFlags |= (byte) (slotFlags & (HMARecordPointer.FLAG_MULTI_SSTABLE
                                                                | HMARecordPointer.FLAG_TWO_SSTABLE
                                                                | HMARecordPointer.FLAG_BASE_COMPLETE));
                            if ((mergedFlags & HMARecordPointer.FLAG_MULTI_BLOCK) != 0)
                                mergedFlags &= ~HMARecordPointer.FLAG_TWO_SSTABLE;
                            newBaseSstableId = existingBaseSstableId;
                            newBaseDataOffset = existingBaseDataOffset;
                        }
                        else if ((slotFlags & HMARecordPointer.FLAG_MULTI_SSTABLE) == 0)
                        {
                            mergedFlags |= HMARecordPointer.FLAG_MULTI_SSTABLE;
                            if ((slotFlags & HMARecordPointer.FLAG_MULTI_BLOCK) == 0
                                && (flags & HMARecordPointer.FLAG_MULTI_BLOCK) == 0
                                && existingSstableId > 0 && existingSstableId <= Integer.MAX_VALUE
                                && existingDataOffset >= 0)
                            {
                                newBaseSstableId = (int) existingSstableId;
                                newBaseDataOffset = existingDataOffset;
                                mergedFlags |= HMARecordPointer.FLAG_TWO_SSTABLE;
                                if (existingComplete)
                                    mergedFlags |= HMARecordPointer.FLAG_BASE_COMPLETE;
                            }
                        }
                        else
                        {
                            mergedFlags |= HMARecordPointer.FLAG_MULTI_SSTABLE;
                            mergedFlags &= ~HMARecordPointer.FLAG_TWO_SSTABLE;
                            if ((slotFlags & HMARecordPointer.FLAG_BASE_COMPLETE) != 0 && existingBaseSstableId > 0)
                            {
                                newBaseSstableId = existingBaseSstableId;
                                newBaseDataOffset = existingBaseDataOffset;
                                mergedFlags |= HMARecordPointer.FLAG_BASE_COMPLETE;
                            }
                        }
                    }

                    // Overwrite slot with latest record pointer
                    writeEntry(mem, offset, hashHi, hashLo, sstableId, dataOffset, dataLength, timestamp, generation, mergedFlags, newBaseSstableId, newBaseDataOffset);

                    if (owner != null && !samePhysicalRecord)
                    {
                        if (sstableId != 0L && (mergedFlags & HMARecordPointer.FLAG_TOMBSTONE) == 0)
                        {
                            owner.onRecordWritten(sstableId, dataOffset, dataLength, true);
                        }
                        if (existingSstableId != 0L
                            && (slotFlags & HMARecordPointer.FLAG_TOMBSTONE) == 0
                            && ((mergedFlags & HMARecordPointer.FLAG_MULTI_SSTABLE) == 0 || isDefragOrCompactionMove))
                        {
                            owner.onRecordSuperseded(existingSstableId, existingDataOffset, existingDataLength);
                        }
                    }
                    return true;
                }

                idx = (idx + 1) & mask;
            }

            if (firstAvailableSlot != -1)
            {
                long offset = (long) firstAvailableSlot * HMARecordPointer.ENTRY_SIZE;
                writeEntry(mem, offset, hashHi, hashLo, sstableId, dataOffset, dataLength, timestamp, generation, flags, 0, -1L);
                size++;
                if (owner != null && sstableId != 0L && (flags & HMARecordPointer.FLAG_TOMBSTONE) == 0)
                {
                    owner.onRecordWritten(sstableId, dataOffset, dataLength, true);
                }
                return true;
            }

            // Fallback: force resize if table full
            resize(capacity << 1);
            return put(hashHi, hashLo, sstableId, dataOffset, dataLength, timestamp, generation, flags, owner);
        }
        finally
        {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Mark a key as tombstone.
     */
    public boolean delete(long hashHi, long hashLo, long timestamp)
    {
        return delete(hashHi, hashLo, timestamp, null);
    }

    /**
     * Mark a key as tombstone and update block-level defrag accounting.
     */
    public boolean delete(long hashHi, long hashLo, long timestamp, HMAPrimaryIndex owner)
    {
        return put(hashHi,
                   hashLo,
                   0L,
                   0L,
                   0,
                   timestamp,
                   0,
                   (byte) (HMARecordPointer.FLAG_IN_USE | HMARecordPointer.FLAG_TOMBSTONE | HMARecordPointer.FLAG_COMPLETE_OVERWRITE),
                   owner);
    }

    private void writeEntry(Memory mem,
                            long offset,
                            long hashHi,
                            long hashLo,
                            long sstableId,
                            long dataOffset,
                            int dataLength,
                            long timestamp,
                            int generation,
                            byte flags,
                            int baseSstableId,
                            long baseDataOffset)
    {
        mem.setLong(offset + HMARecordPointer.OFFSET_KEY_HASH_HI, hashHi);
        mem.setLong(offset + HMARecordPointer.OFFSET_KEY_HASH_LO, hashLo);
        mem.setLong(offset + HMARecordPointer.OFFSET_SSTABLE_ID, sstableId);
        mem.setLong(offset + HMARecordPointer.OFFSET_DATA_OFFSET, dataOffset);
        mem.setInt(offset + HMARecordPointer.OFFSET_DATA_LENGTH, dataLength);
        mem.setInt(offset + HMARecordPointer.OFFSET_GENERATION, generation);
        mem.setLong(offset + HMARecordPointer.OFFSET_TIMESTAMP, timestamp);
        mem.setByte(offset + HMARecordPointer.OFFSET_FLAGS, (byte) (flags | HMARecordPointer.FLAG_IN_USE));
        mem.setInt(offset + HMARecordPointer.OFFSET_BASE_SSTABLE_ID, baseSstableId);
        mem.setLong(offset + HMARecordPointer.OFFSET_BASE_DATA_OFFSET, baseDataOffset);
    }

    private void resize(int newCapacity)
    {
        long newBytes = (long) newCapacity * HMARecordPointer.ENTRY_SIZE;
        Memory newMemory = Memory.allocate(newBytes);
        newMemory.setMemory(0, newBytes, (byte) 0);

        int newMask = newCapacity - 1;
        Memory oldMemory = this.memory;
        int oldCapacity = this.capacity;

        for (int i = 0; i < oldCapacity; i++)
        {
            long oldOffset = (long) i * HMARecordPointer.ENTRY_SIZE;
            byte flags = oldMemory.getByte(oldOffset + HMARecordPointer.OFFSET_FLAGS);

            if ((flags & HMARecordPointer.FLAG_IN_USE) != 0)
            {
                long hi = oldMemory.getLong(oldOffset + HMARecordPointer.OFFSET_KEY_HASH_HI);
                long lo = oldMemory.getLong(oldOffset + HMARecordPointer.OFFSET_KEY_HASH_LO);
                long sstableId = oldMemory.getLong(oldOffset + HMARecordPointer.OFFSET_SSTABLE_ID);
                long dataOffset = oldMemory.getLong(oldOffset + HMARecordPointer.OFFSET_DATA_OFFSET);
                int dataLength = oldMemory.getInt(oldOffset + HMARecordPointer.OFFSET_DATA_LENGTH);
                int generation = oldMemory.getInt(oldOffset + HMARecordPointer.OFFSET_GENERATION);
                long timestamp = oldMemory.getLong(oldOffset + HMARecordPointer.OFFSET_TIMESTAMP);
                int baseSstableId = oldMemory.getInt(oldOffset + HMARecordPointer.OFFSET_BASE_SSTABLE_ID);
                long baseDataOffset = oldMemory.getLong(oldOffset + HMARecordPointer.OFFSET_BASE_DATA_OFFSET);

                int newIdx = (int) (hi ^ (hi >>> 32) ^ lo) & newMask;
                while (newMemory.getByte((long) newIdx * HMARecordPointer.ENTRY_SIZE + HMARecordPointer.OFFSET_FLAGS) != HMARecordPointer.FLAG_EMPTY)
                {
                    newIdx = (newIdx + 1) & newMask;
                }

                long destOffset = (long) newIdx * HMARecordPointer.ENTRY_SIZE;
                writeEntry(newMemory, destOffset, hi, lo, sstableId, dataOffset, dataLength, timestamp, generation, flags, baseSstableId, baseDataOffset);
            }
        }

        // Release old memory
        oldMemory.free();

        this.memory = newMemory;
        this.capacity = newCapacity;
        this.mask = newMask;
        this.threshold = (int) (newCapacity * LOAD_FACTOR);
    }

    private void checkNotClosed()
    {
        if (isClosed)
            throw new IllegalStateException("HMAPrimaryIndexShard is closed");
    }

    @Override
    public void close()
    {
        long stamp = lock.writeLock();
        try
        {
            if (!isClosed)
            {
                isClosed = true;
                if (memory != null)
                {
                    memory.free();
                    memory = null;
                }
                size = 0;
            }
        }
        finally
        {
            lock.unlockWrite(stamp);
        }
    }
}
