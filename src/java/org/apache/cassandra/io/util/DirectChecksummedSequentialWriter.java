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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.sun.nio.file.ExtendedOpenOption;
import org.agrona.BitUtil;
import sun.nio.ch.DirectBuffer;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.hma.HMADirectIOEngine;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.utils.Throwables.merge;

/**
 * Uncompressed SSTable {@link ChecksummedSequentialWriter} that writes directly to storage via
 * {@link ExtendedOpenOption#DIRECT} ({@code O_DIRECT}) using 4KB hardware-aligned off-heap buffers,
 * completely bypassing the OS Page Cache on the write path (Part 3).
 */
public class DirectChecksummedSequentialWriter extends ChecksummedSequentialWriter
{
    private final int blockSize;
    private final int directBufferBytes;
    private long actualDataSize = 0;

    public DirectChecksummedSequentialWriter(File file,
                                             File crcPath,
                                             File digestFile,
                                             SequentialWriterOption option)
    {
        this(file, crcPath, digestFile, option, HMADirectIOEngine.instance().resolveBlockSize(file));
    }

    private DirectChecksummedSequentialWriter(File file,
                                              File crcPath,
                                              File digestFile,
                                              SequentialWriterOption option,
                                              int blockSize)
    {
        super(file,
              HMADirectIOEngine.instance().allocateAlignedBuffer(Math.max(option.bufferSize(), blockSize * 16), blockSize),
              crcPath,
              digestFile,
              option,
              ExtendedOpenOption.DIRECT);
        this.blockSize = blockSize;
        this.directBufferBytes = buffer.capacity();
        StorageMetrics.directWriteBufferBytes.inc(directBufferBytes);
        StorageMetrics.directWriteBuffersAllocated.mark();
    }

    @Override
    protected void flushData()
    {
        int validBytes = buffer.position();
        if (validBytes == 0)
            return;

        // 1. Compute checksum over the exact unpadded payload bytes [0, validBytes)
        ByteBuffer toAppend = buffer.duplicate();
        toAppend.position(0);
        toAppend.limit(validBytes);
        crcMetadata.appendDirect(toAppend, false);

        try
        {
            if ((validBytes & (blockSize - 1)) == 0)
            {
                buffer.flip();
                while (buffer.hasRemaining())
                    fchannel.write(buffer);

                actualDataSize += validBytes;
                lastFlushOffset += validBytes;
                HMADirectIOEngine.instance().recordDirectWrite(validBytes);
            }
            else
            {
                // Sub-block tail flush: pad with zeroes to block boundary for O_DIRECT, write, and truncate back
                int alignedLimit = BitUtil.align(validBytes, blockSize);
                ByteBufferUtil.writeZeroes(buffer, alignedLimit - validBytes);
                buffer.position(0);
                buffer.limit(alignedLimit);
                while (buffer.hasRemaining())
                    fchannel.write(buffer);

                actualDataSize += validBytes;
                lastFlushOffset += validBytes;
                fchannel.truncate(actualDataSize);
                fchannel.position(actualDataSize);

                // Restore buffer position to validBytes so SequentialWriter.resetBuffer() advances bufferOffset accurately
                buffer.limit(buffer.capacity());
                buffer.position(validBytes);
                HMADirectIOEngine.instance().recordDirectWrite(alignedLimit);
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }

        if (runPostFlush != null)
            runPostFlush.accept(getLastFlushOffset());
    }

    protected class DirectTransactionalProxy extends ChecksummedSequentialWriter.TransactionalProxy
    {
        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            if (buffer != null)
            {
                try
                {
                    Object attachment = ((DirectBuffer) buffer).attachment();
                    if (attachment instanceof ByteBuffer)
                        MemoryUtil.clean((ByteBuffer) attachment);
                    else
                        MemoryUtil.clean(buffer);
                }
                catch (Throwable t)
                {
                    accumulate = merge(accumulate, t);
                }
                StorageMetrics.directWriteBufferBytes.dec(directBufferBytes);
                buffer = null;
            }
            return super.doPreCleanup(accumulate);
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new DirectTransactionalProxy();
    }
}
