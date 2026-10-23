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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HMAFastCasEngineTest extends CQLTester
{
    @BeforeClass
    public static void setupHMA()
    {
        HMAFastCasEngine.setEnabled(true);
    }

    @Test
    public void testFastCasSuccessAndFailureAndConcurrency() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, version bigint, balance map<text, bigint>, profile blob)");

        long beforeSuccess = HMAFastCasEngine.getFastCasSuccessCount();
        long beforeFail = HMAFastCasEngine.getFastCasConditionNotMetCount();

        Map<String, Long> initBalance = new HashMap<>();
        initBalance.put("b0", 1000L);
        initBalance.put("b1", 2000L);
        ByteBuffer blob = ByteBuffer.wrap(new byte[3800]);

        execute("INSERT INTO %s (pk, version, balance, profile) VALUES (?, ?, ?, ?)",
                "user-1", 1L, initBalance, blob);

        // 1. Matching CAS condition (version = 1 AND balance['b0'] != null) -> should succeed
        UntypedResultSet rs1 = execute("UPDATE %s SET version = ?, balance[?] = ? WHERE pk = ? IF version = ? AND balance[?] != null",
                                       2L, "b0", 1500L, "user-1", 1L, "b0");
        assertNotNull(rs1);
        UntypedResultSet.Row row1 = rs1.one();
        assertTrue(row1.getBoolean("[applied]"));
        assertTrue(HMAFastCasEngine.getFastCasSuccessCount() > beforeSuccess);

        // Verify updated values
        UntypedResultSet check1 = execute("SELECT version, balance FROM %s WHERE pk = ?", "user-1");
        UntypedResultSet.Row checkRow1 = check1.one();
        assertEquals(2L, checkRow1.getLong("version"));
        assertEquals(Long.valueOf(1500L), checkRow1.getMap("balance", org.apache.cassandra.db.marshal.UTF8Type.instance, org.apache.cassandra.db.marshal.LongType.instance).get("b0"));

        // 2. Stale CAS condition (version = 1 when current is 2) -> should fail and return current version = 2
        UntypedResultSet rs2 = execute("UPDATE %s SET version = ?, balance[?] = ? WHERE pk = ? IF version = ? AND balance[?] != null",
                                       3L, "b0", 9999L, "user-1", 1L, "b0");
        UntypedResultSet.Row row2 = rs2.one();
        assertFalse(row2.getBoolean("[applied]"));
        assertEquals(2L, row2.getLong("version"));
        assertTrue(HMAFastCasEngine.getFastCasConditionNotMetCount() > beforeFail);

        // 3. Concurrent contention on same key: 8 threads racing to transition version from 2 -> 3
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger winners = new AtomicInteger(0);

        for (int i = 0; i < threads; i++)
        {
            final long delta = (i + 1) * 100L;
            pool.submit(() -> {
                try
                {
                    startLatch.await();
                    UntypedResultSet res = execute("UPDATE %s SET version = ?, balance[?] = ? WHERE pk = ? IF version = ?",
                                                   3L, "b0", 1500L + delta, "user-1", 2L);
                    if (res.one().getBoolean("[applied]"))
                        winners.incrementAndGet();
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }
                finally
                {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        pool.shutdownNow();

        // Exactly 1 thread must win the CAS transition 2 -> 3
        assertEquals(1, winners.get());
        UntypedResultSet check2 = execute("SELECT version FROM %s WHERE pk = ?", "user-1");
        assertEquals(3L, check2.one().getLong("version"));

        // 4. Flush and verify full-row merge preserves all non-updated columns across SSTables
        flush();
        execute("SELECT version FROM %s WHERE pk = ?", "user-1");
        UntypedResultSet rs3 = execute("UPDATE %s SET version = ?, balance[?] = ? WHERE pk = ? IF version = ?",
                                       4L, "b0", 3000L, "user-1", 3L);
        assertTrue(rs3.one().getBoolean("[applied]"));
        flush();

        UntypedResultSet fullRow = execute("SELECT pk, version, balance, profile FROM %s WHERE pk = ?", "user-1");
        UntypedResultSet.Row r = fullRow.one();
        assertNotNull(r);
        assertEquals(4L, r.getLong("version"));
        Map<String, Long> finalBal = r.getMap("balance", org.apache.cassandra.db.marshal.UTF8Type.instance, org.apache.cassandra.db.marshal.LongType.instance);
        assertEquals(Long.valueOf(3000L), finalBal.get("b0"));
        assertEquals(Long.valueOf(2000L), finalBal.get("b1"));
        assertEquals(3800, r.getBytes("profile").remaining());
    }
}
