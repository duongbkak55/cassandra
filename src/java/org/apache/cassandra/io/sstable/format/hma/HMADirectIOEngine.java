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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import com.sun.nio.file.ExtendedOpenOption;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DirectThreadLocalByteBufferHolder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.SyncUtil;

/**
 * Aerospike-inspired Direct I/O & Asynchronous I/O Engine for Cassandra (Part 3).
 *
 * <p>Eliminates OS Page Cache double-buffering and compaction cache eviction pollution by:
 * <ul>
 *   <li>Opening SSTable data channels with {@link ExtendedOpenOption#DIRECT} ({@code O_DIRECT} on Linux,
 *       {@code F_NOCACHE} on macOS) with automatic fallback when a virtual/tmpfs filesystem does not
 *       support {@code O_DIRECT}.</li>
 *   <li>Enforcing hardware sector/page alignment (4096-byte aligned memory addresses and file offsets)
 *       via {@link BufferUtil#allocateDirectAligned(int, int)} and per-blocksize
 *       {@link DirectThreadLocalByteBufferHolder}.</li>
 *   <li>Providing an asynchronous I/O submission ring / worker pool for parallel NVMe queue-depth dispatch
 *       without blocking caller coordinator threads.</li>
 * </ul>
 */
public final class HMADirectIOEngine
{
    private static final Logger logger = LoggerFactory.getLogger(HMADirectIOEngine.class);

    public static final String ENABLE_DIRECT_IO_PROP = "cassandra.hma.direct_io.enabled";
    public static final String ASYNC_IO_THREADS_PROP = "cassandra.hma.direct_io.async_threads";
    public static final int DEFAULT_BLOCK_SIZE = 4096;

    private static final HMADirectIOEngine INSTANCE = new HMADirectIOEngine();

    private final AtomicBoolean directIOEnabled = new AtomicBoolean(
        Boolean.parseBoolean(System.getProperty(ENABLE_DIRECT_IO_PROP, "true"))
    );

    private final ConcurrentHashMap<Integer, DirectThreadLocalByteBufferHolder> alignedHolders = new ConcurrentHashMap<>();
    private final ExecutorService asyncIORingExecutor;

    private final LongAdder directIOReads = new LongAdder();
    private final LongAdder directIOWrites = new LongAdder();
    private final LongAdder directIOBytesRead = new LongAdder();
    private final LongAdder directIOBytesWritten = new LongAdder();
    private final LongAdder alignedBlocksAllocated = new LongAdder();
    private final LongAdder asyncIOSubmitted = new LongAdder();
    private final LongAdder asyncIOCompleted = new LongAdder();

    private HMADirectIOEngine()
    {
        int threads = Integer.getInteger(ASYNC_IO_THREADS_PROP,
                                         Math.max(4, Runtime.getRuntime().availableProcessors()));
        AtomicInteger threadIdx = new AtomicInteger(1);
        this.asyncIORingExecutor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "HMA-DirectIO-Ring-" + threadIdx.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
    }

    public static HMADirectIOEngine instance()
    {
        return INSTANCE;
    }

    public boolean isDirectIOEnabled()
    {
        return directIOEnabled.get() && HMAPrimaryIndexManager.instance().isEnabled();
    }

    public void setDirectIOEnabled(boolean enabled)
    {
        directIOEnabled.set(enabled);
    }

    /**
     * Resolves the filesystem block size for Direct I/O alignment, defaulting to 4096 bytes
     * if the platform returns a non-power-of-two or non-positive value.
     */
    public int resolveBlockSize(File file)
    {
        try
        {
            File parent = file != null ? file.parent() : null;
            if (parent != null && parent.exists())
            {
                int bs = FileUtils.getBlockSize(parent);
                if (bs > 0 && BitUtil.isPowerOfTwo(bs))
                    return Math.max(bs, DEFAULT_BLOCK_SIZE);
            }
        }
        catch (Throwable ignored)
        {
        }
        return DEFAULT_BLOCK_SIZE;
    }

    /**
     * Returns a thread-local holder for 4KB-aligned off-heap ByteBuffers.
     */
    public DirectThreadLocalByteBufferHolder getAlignedBufferHolder(int blockSize)
    {
        int validBlockSize = (blockSize > 0 && BitUtil.isPowerOfTwo(blockSize)) ? blockSize : DEFAULT_BLOCK_SIZE;
        return alignedHolders.computeIfAbsent(validBlockSize, DirectThreadLocalByteBufferHolder::new);
    }

    /**
     * Allocates a hardware-aligned off-heap ByteBuffer whose memory address and capacity
     * are multiples of {@code blockSize}.
     */
    public ByteBuffer allocateAlignedBuffer(int capacity, int blockSize)
    {
        int validBlockSize = (blockSize > 0 && BitUtil.isPowerOfTwo(blockSize)) ? blockSize : DEFAULT_BLOCK_SIZE;
        int alignedCap = BitUtil.align(Math.max(capacity, validBlockSize), validBlockSize);
        alignedBlocksAllocated.increment();
        return BufferUtil.allocateDirectAligned(alignedCap, validBlockSize);
    }

    /**
     * Opens a read-only {@link FileChannel} with {@link ExtendedOpenOption#DIRECT}, falling back
     * gracefully to standard read channel if the underlying filesystem rejects {@code O_DIRECT}.
     */
    public static FileChannel openReadChannelWithFallback(File file, OpenOption... openOptions) throws IOException
    {
        try
        {
            return FileChannel.open(file.toPath(), openOptions);
        }
        catch (IOException | UnsupportedOperationException e)
        {
            boolean hadDirect = false;
            List<OpenOption> fallback = new ArrayList<>(openOptions.length);
            for (OpenOption opt : openOptions)
            {
                if (opt == ExtendedOpenOption.DIRECT)
                    hadDirect = true;
                else
                    fallback.add(opt);
            }
            if (!hadDirect)
                throw e;

            logger.debug("Filesystem for {} does not support ExtendedOpenOption.DIRECT ({}); falling back to aligned direct channel",
                         file, e.getMessage());
            return FileChannel.open(file.toPath(), fallback.toArray(OpenOption[]::new));
        }
    }

    /**
     * Opens a read-write {@link FileChannel} for sequential writing with {@link ExtendedOpenOption#DIRECT},
     * falling back gracefully if the underlying filesystem rejects {@code O_DIRECT}.
     */
    public static FileChannel openWriteChannelWithFallback(File file, OpenOption... extraOptions) throws IOException
    {
        Set<OpenOption> options = new LinkedHashSet<>(Arrays.asList(StandardOpenOption.READ, StandardOpenOption.WRITE));
        options.addAll(Arrays.asList(extraOptions));
        boolean createNew = !file.exists();
        if (createNew)
            options.add(StandardOpenOption.CREATE_NEW);

        try
        {
            FileChannel channel = FileChannel.open(file.toPath(), options.toArray(OpenOption[]::new));
            if (createNew)
                syncParentDirOrClose(file, channel);
            return channel;
        }
        catch (IOException | UnsupportedOperationException e)
        {
            if (!options.remove(ExtendedOpenOption.DIRECT))
                throw e;

            logger.debug("Filesystem for {} does not support ExtendedOpenOption.DIRECT on write ({}); falling back to aligned direct channel",
                         file, e.getMessage());
            FileChannel channel = FileChannel.open(file.toPath(), options.toArray(OpenOption[]::new));
            if (createNew)
                syncParentDirOrClose(file, channel);
            return channel;
        }
    }

    private static void syncParentDirOrClose(File file, FileChannel channel)
    {
        try
        {
            SyncUtil.trySyncDir(file.parent());
        }
        catch (Throwable t)
        {
            try
            {
                channel.close();
            }
            catch (Throwable t2)
            {
                t.addSuppressed(t2);
            }
            throw t;
        }
    }

    /**
     * Performs a 4KB block-aligned {@code O_DIRECT} positional read from {@code channel} and copies
     * the exact record slice {@code [position, position + length)} into {@code dest}.
     *
     * @return the number of unpadded payload bytes transferred into {@code dest}
     */
    public int readAlignedChunk(ChannelProxy channel, long position, ByteBuffer dest, int blockSize)
    {
        int requestedLength = dest.remaining();
        if (requestedLength <= 0)
            return 0;

        int validBlockSize = (blockSize > 0 && BitUtil.isPowerOfTwo(blockSize)) ? blockSize : DEFAULT_BLOCK_SIZE;
        long alignedPos = position & -(long) validBlockSize;
        int delta = (int) (position - alignedPos);
        int alignedLength = BitUtil.align(delta + requestedLength, validBlockSize);

        DirectThreadLocalByteBufferHolder holder = getAlignedBufferHolder(validBlockSize);
        ByteBuffer alignedBuf = holder.getBuffer(alignedLength);
        alignedBuf.clear();
        alignedBuf.limit(alignedLength);

        int rawBytesRead = channel.read(alignedBuf, alignedPos);
        if (rawBytesRead <= delta)
        {
            recordDirectRead(Math.max(0, rawBytesRead));
            return 0;
        }

        int availablePayload = Math.min(requestedLength, rawBytesRead - delta);
        alignedBuf.position(delta);
        alignedBuf.limit(delta + availablePayload);
        dest.put(alignedBuf);

        recordDirectRead(rawBytesRead);
        return availablePayload;
    }

    /**
     * Submits an asynchronous block-aligned Direct I/O point-read to the non-blocking NVMe worker ring.
     */
    public CompletableFuture<ByteBuffer> submitAsyncDirectRead(ChannelProxy channel,
                                                               long fileOffset,
                                                               int length,
                                                               int blockSize)
    {
        asyncIOSubmitted.increment();
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                ByteBuffer result = ByteBuffer.allocateDirect(length);
                readAlignedChunk(channel, fileOffset, result, blockSize);
                result.flip();
                return result;
            }
            finally
            {
                asyncIOCompleted.increment();
            }
        }, asyncIORingExecutor);
    }

    /**
     * Dispatches a batch of Direct I/O point reads in parallel across the async I/O ring
     * to saturate NVMe multi-queue concurrency without blocking the caller thread per read.
     */
    public List<ByteBuffer> readBatchAsync(ChannelProxy channel, long[] offsets, int[] lengths, int blockSize)
    {
        if (offsets.length != lengths.length)
            throw new IllegalArgumentException("offsets and lengths must have identical length");

        List<CompletableFuture<ByteBuffer>> futures = new ArrayList<>(offsets.length);
        for (int i = 0; i < offsets.length; i++)
        {
            futures.add(submitAsyncDirectRead(channel, offsets[i], lengths[i], blockSize));
        }

        List<ByteBuffer> results = new ArrayList<>(offsets.length);
        for (CompletableFuture<ByteBuffer> f : futures)
        {
            results.add(f.join());
        }
        return results;
    }

    public void recordDirectRead(long bytes)
    {
        directIOReads.increment();
        if (bytes > 0)
            directIOBytesRead.add(bytes);
    }

    public void recordDirectWrite(long bytes)
    {
        directIOWrites.increment();
        if (bytes > 0)
            directIOBytesWritten.add(bytes);
    }

    public long getDirectIOReads()
    {
        return directIOReads.sum();
    }

    public long getDirectIOWrites()
    {
        return directIOWrites.sum();
    }

    public long getDirectIOBytesRead()
    {
        return directIOBytesRead.sum();
    }

    public long getDirectIOBytesWritten()
    {
        return directIOBytesWritten.sum();
    }

    public long getAlignedBlocksAllocated()
    {
        return alignedBlocksAllocated.sum();
    }

    public long getAsyncIOSubmitted()
    {
        return asyncIOSubmitted.sum();
    }

    public long getAsyncIOCompleted()
    {
        return asyncIOCompleted.sum();
    }

    public void resetMetrics()
    {
        directIOReads.reset();
        directIOWrites.reset();
        directIOBytesRead.reset();
        directIOBytesWritten.reset();
        alignedBlocksAllocated.reset();
        asyncIOSubmitted.reset();
        asyncIOCompleted.reset();
    }
}
