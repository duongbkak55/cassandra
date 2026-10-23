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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import net.nicoulaj.compilecommand.annotations.Inline;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AddressBasedAllocator;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativeEndianMemoryUtil;

public class NativeDecoratedKey extends DecoratedKey
{
    final long peer;

    public NativeDecoratedKey(Token token, NativeAllocator allocator, OpOrder.Group writeOp, ByteBuffer key)
    {
        this(token, (AddressBasedAllocator) allocator, writeOp, key);
    }

    public NativeDecoratedKey(Token token, AddressBasedAllocator allocator, OpOrder.Group writeOp, ByteBuffer key)
    {
        super(token);
        assert key != null;
        assert key.order() == ByteOrder.BIG_ENDIAN;

        int size = key.remaining();
        this.peer = allocator.allocate(4 + size, writeOp);
        NativeEndianMemoryUtil.setInt(peer, size);
        MemoryUtil.setBytes(peer + 4, key);
    }

    public NativeDecoratedKey(Token token, NativeAllocator allocator, OpOrder.Group writeOp, byte[] keyBytes)
    {
        this(token, (AddressBasedAllocator) allocator, writeOp, keyBytes);
    }

    public NativeDecoratedKey(Token token, AddressBasedAllocator allocator, OpOrder.Group writeOp, byte[] keyBytes)
    {
        super(token);
        assert keyBytes != null;

        int size = keyBytes.length;
        this.peer = allocator.allocate(4 + size, writeOp);
        NativeEndianMemoryUtil.setInt(peer, size);
        MemoryUtil.setBytes(peer + 4, keyBytes, 0, size);
    }

    @Inline
    int length()
    {
        return NativeEndianMemoryUtil.getInt(peer);
    }

    @Inline
    long address()
    {
        return this.peer + 4;
    }

    @Override
    public ByteBuffer getKey()
    {
        return MemoryUtil.getByteBuffer(address(), length(), ByteOrder.BIG_ENDIAN);
    }

    @Override
    public int getKeyLength()
    {
        return NativeEndianMemoryUtil.getInt(peer);
    }

    @Override
    public int hashCode()
    {
        long addr = address();
        int len = length();
        int h = 1;
        for (int i = len - 1; i >= 0; i--)
            h = 31 * h + (int) MemoryUtil.getByte(addr + i);
        return h;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!(obj instanceof DecoratedKey))
            return false;
        return compareBytesOnly((DecoratedKey) obj) == 0;
    }

    @Override
    public int compareTo(PartitionPosition pos)
    {
        if (this == pos)
            return 0;
        if (!(pos instanceof DecoratedKey))
            return -pos.compareTo(this);
        DecoratedKey otherKey = (DecoratedKey) pos;
        int cmp = getToken().compareTo(otherKey.getToken());
        return cmp == 0 ? compareBytesOnly(otherKey) : cmp;
    }

    @Override
    public int compareBytesOnly(DecoratedKey that)
    {
        if (this == that)
            return 0;
        if (that instanceof NativeDecoratedKey)
        {
            NativeDecoratedKey other = (NativeDecoratedKey) that;
            return FastByteOperations.compareMemoryUnsigned(address(), length(), other.address(), other.length());
        }
        return -FastByteOperations.compareWithMemoryUnsigned(that.getKey(), address(), length());
    }

    @Override
    protected ByteSource keyComparableBytes(Version version)
    {
        return ByteSource.ofMemory(address(), length(), version);
    }
}
