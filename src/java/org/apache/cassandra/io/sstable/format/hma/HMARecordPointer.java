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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.utils.MurmurHash;

/**
 * 64-byte Cache-Line Aligned Record Pointer inspired by Aerospike's {@code as_index}.
 *
 * <p>Memory layout (total 64 bytes = 1 CPU cache line):
 * <pre>
 * Offset  Size  Description
 * -------------------------------------------------------------
 * 0       8     Key Hash Hi (Murmur3 128-bit hi component / token)
 * 8       8     Key Hash Lo (Murmur3 128-bit lo component)
 * 16      8     SSTable ID (numeric generation / identifier)
 * 24      8     Data Offset (byte offset inside Data.db / write block)
 * 32      4     Data Length (serialized payload byte size)
 * 36      4     Record Generation / Sequence number
 * 40      8     Timestamp (microsecond timestamp for Last-Write-Wins)
 * 48      1     Flags (IN_USE, TOMBSTONE, etc.)
 * 49      15    Reserved / Alignment padding (pads exactly to 64 bytes)
 * </pre>
 */
public final class HMARecordPointer
{
    public static final int ENTRY_SIZE = 64;

    // Field byte offsets within 64-byte entry
    public static final int OFFSET_KEY_HASH_HI = 0;
    public static final int OFFSET_KEY_HASH_LO = 8;
    public static final int OFFSET_SSTABLE_ID = 16;
    public static final int OFFSET_DATA_OFFSET = 24;
    public static final int OFFSET_DATA_LENGTH = 32;
    public static final int OFFSET_GENERATION = 36;
    public static final int OFFSET_TIMESTAMP = 40;
    public static final int OFFSET_FLAGS = 48;
    public static final int OFFSET_RESERVED = 49;
    public static final int OFFSET_BASE_SSTABLE_ID = 52;
    public static final int OFFSET_BASE_DATA_OFFSET = 56;

    // Flags
    public static final byte FLAG_EMPTY              = 0x00;
    public static final byte FLAG_IN_USE             = 0x01;
    public static final byte FLAG_TOMBSTONE          = 0x02;
    public static final byte FLAG_MULTI_BLOCK        = 0x04;
    public static final byte FLAG_MULTI_SSTABLE      = 0x08;
    public static final byte FLAG_COMPLETE_OVERWRITE = 0x10;
    public static final byte FLAG_TWO_SSTABLE        = 0x20;
    public static final byte FLAG_BASE_COMPLETE      = 0x40;

    // In-memory representation
    public long keyHashHi;
    public long keyHashLo;
    public long sstableId;
    public long dataOffset;
    public int dataLength;
    public int generation;
    public long timestamp;
    public byte flags;
    public int baseSstableId;
    public long baseDataOffset = -1L;

    public HMARecordPointer()
    {
    }

    public HMARecordPointer(long keyHashHi,
                            long keyHashLo,
                            long sstableId,
                            long dataOffset,
                            int dataLength,
                            int generation,
                            long timestamp,
                            byte flags)
    {
        this.keyHashHi = keyHashHi;
        this.keyHashLo = keyHashLo;
        this.sstableId = sstableId;
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
        this.generation = generation;
        this.timestamp = timestamp;
        this.flags = flags;
        this.baseSstableId = 0;
        this.baseDataOffset = -1L;
    }

    public boolean isInUse()
    {
        return (flags & FLAG_IN_USE) != 0;
    }

    public boolean isTombstone()
    {
        return (flags & FLAG_TOMBSTONE) != 0;
    }

    public boolean isMultiBlock()
    {
        return (flags & FLAG_MULTI_BLOCK) != 0;
    }

    public boolean isMultiSSTable()
    {
        return (flags & FLAG_MULTI_SSTABLE) != 0;
    }

    public boolean isTwoSSTable()
    {
        return (flags & FLAG_TWO_SSTABLE) != 0;
    }

    public boolean isBaseComplete()
    {
        return (flags & FLAG_BASE_COMPLETE) != 0;
    }

    /**
     * Deterministically map a Cassandra {@link SSTableId} to a 64-bit integer for off-heap storage.
     */
    public static long resolveSSTableId(SSTableId id)
    {
        if (id == null)
            return 0L;
        if (id instanceof SequenceBasedSSTableId)
            return ((SequenceBasedSSTableId) id).generation;
        if (id instanceof UUIDBasedSSTableId)
        {
            ByteBuffer bytes = id.asBytes();
            return (bytes.getLong(0) ^ bytes.getLong(8)) | Long.MIN_VALUE;
        }
        return id.hashCode();
    }

    /**
     * Deterministically map an SSTable {@link Descriptor} to a 64-bit integer for off-heap storage.
     */
    public static long resolveSSTableId(Descriptor descriptor)
    {
        return descriptor != null ? resolveSSTableId(descriptor.id) : 0L;
    }

    /**
     * Compute 128-bit key hash for a DecoratedKey using MurmurHash3.
     */
    public static void computeKeyHash(DecoratedKey key, long[] outHash128)
    {
        ByteBuffer buffer = key.getKey();
        int pos = buffer.position();
        int rem = buffer.remaining();
        MurmurHash.hash3_x64_128(buffer, pos, rem, 0, outHash128);
    }

    /**
     * Copy all fields from another {@link HMARecordPointer} instance without off-heap access.
     */
    public void copyFrom(HMARecordPointer other)
    {
        this.keyHashHi = other.keyHashHi;
        this.keyHashLo = other.keyHashLo;
        this.sstableId = other.sstableId;
        this.dataOffset = other.dataOffset;
        this.dataLength = other.dataLength;
        this.generation = other.generation;
        this.timestamp = other.timestamp;
        this.flags = other.flags;
        this.baseSstableId = other.baseSstableId;
        this.baseDataOffset = other.baseDataOffset;
    }

    /**
     * Read an entry from an off-heap Memory buffer at the given byte offset.
     */
    public void readFrom(Memory memory, long byteOffset)
    {
        this.keyHashHi = memory.getLong(byteOffset + OFFSET_KEY_HASH_HI);
        this.keyHashLo = memory.getLong(byteOffset + OFFSET_KEY_HASH_LO);
        this.sstableId = memory.getLong(byteOffset + OFFSET_SSTABLE_ID);
        this.dataOffset = memory.getLong(byteOffset + OFFSET_DATA_OFFSET);
        this.dataLength = memory.getInt(byteOffset + OFFSET_DATA_LENGTH);
        this.generation = memory.getInt(byteOffset + OFFSET_GENERATION);
        this.timestamp = memory.getLong(byteOffset + OFFSET_TIMESTAMP);
        this.flags = memory.getByte(byteOffset + OFFSET_FLAGS);
        this.baseSstableId = memory.getInt(byteOffset + OFFSET_BASE_SSTABLE_ID);
        this.baseDataOffset = memory.getLong(byteOffset + OFFSET_BASE_DATA_OFFSET);
    }

    /**
     * Write this entry into an off-heap Memory buffer at the given byte offset.
     */
    public void writeTo(Memory memory, long byteOffset)
    {
        memory.setLong(byteOffset + OFFSET_KEY_HASH_HI, this.keyHashHi);
        memory.setLong(byteOffset + OFFSET_KEY_HASH_LO, this.keyHashLo);
        memory.setLong(byteOffset + OFFSET_SSTABLE_ID, this.sstableId);
        memory.setLong(byteOffset + OFFSET_DATA_OFFSET, this.dataOffset);
        memory.setInt(byteOffset + OFFSET_DATA_LENGTH, this.dataLength);
        memory.setInt(byteOffset + OFFSET_GENERATION, this.generation);
        memory.setLong(byteOffset + OFFSET_TIMESTAMP, this.timestamp);
        memory.setByte(byteOffset + OFFSET_FLAGS, this.flags);
        memory.setInt(byteOffset + OFFSET_BASE_SSTABLE_ID, this.baseSstableId);
        memory.setLong(byteOffset + OFFSET_BASE_DATA_OFFSET, this.baseDataOffset);
    }

    /**
     * Clear a slot at the given byte offset in off-heap memory.
     */
    public static void clearSlot(Memory memory, long byteOffset)
    {
        // Zeroing out flags and key hash is sufficient to mark empty
        memory.setByte(byteOffset + OFFSET_FLAGS, FLAG_EMPTY);
        memory.setLong(byteOffset + OFFSET_KEY_HASH_HI, 0L);
        memory.setLong(byteOffset + OFFSET_KEY_HASH_LO, 0L);
    }

    @Override
    public String toString()
    {
        return String.format("HMARecordPointer{keyHash=(%016X,%016X), sstableId=%d, offset=%d, len=%d, ts=%d, flags=0x%02X}",
                             keyHashHi, keyHashLo, sstableId, dataOffset, dataLength, timestamp, flags);
    }
}
