/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.OrderByFlags;
import com.aerospike.client.query.Reduce;
import com.aerospike.client.query.ReduceSpec;
import com.aerospike.client.query.Statement;

/**
 * Pure client-side unit tests for the {@link ReduceSpec} / {@link Reduce} / {@link Statement}
 * map-reduce framework (see docs/REDUCE-SPEC-DESIGN.md).
 * <p>
 * These tests do not require a running Aerospike cluster: {@link Key} digests are computed
 * locally (RIPEMD-160), and {@link Record} instances are constructed directly. They deliberately
 * do not extend {@link com.aerospike.test.sync.TestSync} / {@link com.aerospike.test.util.TestBase}
 * so they can run standalone without connecting a client, e.g.:
 * <pre>
 *   mvn test -pl test -am -DskipTests=false -DrunSuite=**&#47;TestReduceSpec.class
 * </pre>
 */
public class TestReduceSpec {
	private static final String NS = "test";
	private static final String SET = "reduceset";
	private static final String BIN = "val";

	private static Key key(String userKey) {
		return new Key(NS, SET, userKey);
	}

	private static Record record(String binName, Object value) {
		Map<String, Object> bins = new HashMap<>();
		bins.put(binName, value);
		return new Record(bins, 1, 0);
	}

	//-------------------------------------------------------
	// Top-K reducer
	//-------------------------------------------------------

	@Test
	public void topKReturnsKLargestDescending() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

		for (int i = 1; i <= 10; i++) {
			reducer.acceptPartial(record(BIN, (long)i), key("k" + i));
		}

		Record[] result = reducer.getResult();
		assertEquals(3, result.length);
		assertEquals(10L, result[0].getLong(BIN));
		assertEquals(9L, result[1].getLong(BIN));
		assertEquals(8L, result[2].getLong(BIN));
	}

	@Test
	public void topKReturnsKSmallestAscending() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 3);

		for (int i = 1; i <= 10; i++) {
			reducer.acceptPartial(record(BIN, (long)i), key("k" + i));
		}

		Record[] result = reducer.getResult();
		assertEquals(3, result.length);
		assertEquals(1L, result[0].getLong(BIN));
		assertEquals(2L, result[1].getLong(BIN));
		assertEquals(3L, result[2].getLong(BIN));
	}

	@Test
	public void topKScalarResultIsBestRanked() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

		reducer.acceptPartial(record(BIN, 1L), key("k1"));
		reducer.acceptPartial(record(BIN, 5L), key("k2"));
		reducer.acceptPartial(record(BIN, 3L), key("k3"));

		assertEquals(5L, reducer.getScalarResult().getLong(BIN));
	}

	@Test
	public void topKFewerThanKMatchesReturnsAllOfThem() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);

		reducer.acceptPartial(record(BIN, 1L), key("k1"));
		reducer.acceptPartial(record(BIN, 2L), key("k2"));

		assertEquals(2, reducer.getResult().length);
	}

	@Test
	public void topKDedupesSameDigestKeepingBetterValue() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);

		Key k1 = key("k1");

		// Same key re-scanned (e.g. across a partition migration retry) with a worse, then
		// better, value. Only one entry for k1 should survive, holding the better value.
		reducer.acceptPartial(record(BIN, 1L), k1);
		reducer.acceptPartial(record(BIN, 1L), key("k2"));
		reducer.acceptPartial(record(BIN, 9L), k1);

		Record[] result = reducer.getResult();
		assertEquals(2, result.length);
		assertEquals(9L, result[0].getLong(BIN));
		assertEquals(1L, result[1].getLong(BIN));
	}

	@Test
	public void topKDedupeIgnoresWorseRescan() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);

		Key k1 = key("k1");

		// Better value seen first; a later, worse re-scan of the same key must not replace it.
		reducer.acceptPartial(record(BIN, 9L), k1);
		reducer.acceptPartial(record(BIN, 1L), k1);

		Record[] result = reducer.getResult();
		assertEquals(1, result.length);
		assertEquals(9L, result[0].getLong(BIN));
	}

	@Test
	public void topKTiesBrokenDeterministicallyByDigest() {
		ReduceSpec<Record, Record> a = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2);
		a.acceptPartial(record(BIN, 5L), key("alpha"));
		a.acceptPartial(record(BIN, 5L), key("beta"));
		a.acceptPartial(record(BIN, 5L), key("gamma"));

		ReduceSpec<Record, Record> b = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2);
		b.acceptPartial(record(BIN, 5L), key("gamma"));
		b.acceptPartial(record(BIN, 5L), key("alpha"));
		b.acceptPartial(record(BIN, 5L), key("beta"));

		// Same input set fed in different orders must produce the same result (stable
		// tie-break by digest), regardless of acceptPartial() call order.
		Record[] ra = a.getResult();
		Record[] rb = b.getResult();
		assertEquals(ra.length, rb.length);

		for (int i = 0; i < ra.length; i++) {
			assertEquals(ra[i].getLong(BIN), rb[i].getLong(BIN));
		}
	}

	@Test
	public void topKCaseInsensitiveStringOrdering() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE, 3);

		reducer.acceptPartial(record(BIN, "banana"), key("k1"));
		reducer.acceptPartial(record(BIN, "Apple"), key("k2"));
		reducer.acceptPartial(record(BIN, "cherry"), key("k3"));

		Record[] result = reducer.getResult();
		assertEquals(3, result.length);
		assertEquals("Apple", result[0].getString(BIN));
		assertEquals("banana", result[1].getString(BIN));
		assertEquals("cherry", result[2].getString(BIN));
	}

	@Test
	public void limitRejectsOutOfRangeValues() {
		try {
			Reduce.limit(BIN, 0);
			fail("Expected IllegalArgumentException for limit below 1");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}

		try {
			Reduce.limit(BIN, 1001);
			fail("Expected IllegalArgumentException for limit above 1000");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void orderByAloneThrowsUnsupported() {
		ReduceSpec<Record, Record> orderBy = Reduce.orderBy(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE);

		try {
			orderBy.acceptPartial(record(BIN, 1L), key("k1"));
			fail("Expected UnsupportedOperationException");
		}
		catch (UnsupportedOperationException expected) {
			// pass
		}
	}

	@Test
	public void limitAloneThrowsUnsupported() {
		ReduceSpec<Record, Record> limit = Reduce.limit(BIN, 5);

		try {
			limit.acceptPartial(record(BIN, 1L), key("k1"));
			fail("Expected UnsupportedOperationException");
		}
		catch (UnsupportedOperationException expected) {
			// pass
		}
	}

	//-------------------------------------------------------
	// Statement.setReduce / getReduce / resolveReduce
	//-------------------------------------------------------

	@Test
	public void statementDefaultsToNoReduce() {
		Statement stmt = new Statement();
		assertEquals(null, stmt.resolveReduce());
	}

	@Test
	public void statementResolvesSingleTopK() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));

		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();
		resolved.acceptPartial(record(BIN, 1L), key("k1"));
		resolved.acceptPartial(record(BIN, 2L), key("k2"));
		assertEquals(2, resolved.getResult().length);
	}

	@Test
	public void statementComposesSplitOrderByAndLimit() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.limit(BIN, 2));

		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();

		resolved.acceptPartial(record(BIN, 1L), key("k1"));
		resolved.acceptPartial(record(BIN, 5L), key("k2"));
		resolved.acceptPartial(record(BIN, 3L), key("k3"));

		Record[] result = resolved.getResult();
		assertEquals(2, result.length);
		assertEquals(5L, result[0].getLong(BIN));
		assertEquals(3L, result[1].getLong(BIN));
	}

	@Test
	public void statementSplitOrderIndependent() {
		Statement stmt = new Statement();
		// limit passed before orderBy; order within setReduce should not matter.
		stmt.setReduce(
			Reduce.limit(BIN, 2),
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE));

		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();
		resolved.acceptPartial(record(BIN, 3L), key("k1"));
		resolved.acceptPartial(record(BIN, 1L), key("k2"));

		Record[] result = resolved.getResult();
		assertEquals(2, result.length);
		assertEquals(1L, result[0].getLong(BIN));
	}

	@Test
	public void statementResolveReduceIsMemoized() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.limit(BIN, 2));

		ReduceSpec<Record, Record> first = stmt.resolveReduce();
		ReduceSpec<Record, Record> second = stmt.resolveReduce();

		// Must return the exact same combiner instance across calls, otherwise records fed
		// via one reference would be invisible to a caller holding another reference.
		assertSame(first, second);
	}

	@Test
	public void statementSetReduceResetsMemoizedResolution() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));
		ReduceSpec<Record, Record> first = stmt.resolveReduce();

		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 3));
		ReduceSpec<Record, Record> second = stmt.resolveReduce();

		assertTrue(first != second);
	}

	@Test
	public void statementResolveReduceOrderByWithoutLimitThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE));

		try {
			stmt.resolveReduce();
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void statementResolveReduceComposeBinMismatchThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy("binA", BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.limit("binB", 2));

		try {
			stmt.resolveReduce();
			fail("Expected IllegalArgumentException for mismatched bins");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	//-------------------------------------------------------
	// Statement.setOrderBy / setTopK sugar
	//-------------------------------------------------------

	@Test
	public void setOrderByThenSetTopKResolvesToTopK() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.DOUBLE, Order.ASC);
		stmt.setTopK(2);

		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();
		resolved.acceptPartial(record(BIN, 3.0), key("k1"));
		resolved.acceptPartial(record(BIN, 1.0), key("k2"));
		resolved.acceptPartial(record(BIN, 2.0), key("k3"));

		Record[] result = resolved.getResult();
		assertEquals(2, result.length);
		assertEquals(1.0, result[0].getDouble(BIN), 0.0001);
		assertEquals(2.0, result[1].getDouble(BIN), 0.0001);
	}

	@Test
	public void setOrderByWithFlagsThenSetTopKResolvesToTopK() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE);
		stmt.setTopK(2);

		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();
		resolved.acceptPartial(record(BIN, "banana"), key("k1"));
		resolved.acceptPartial(record(BIN, "Apple"), key("k2"));

		Record[] result = resolved.getResult();
		assertEquals(2, result.length);
		assertEquals("Apple", result[0].getString(BIN));
	}

	@Test
	public void setTopKWithoutSetOrderByThrows() {
		Statement stmt = new Statement();

		try {
			stmt.setTopK(5);
			fail("Expected IllegalStateException");
		}
		catch (IllegalStateException expected) {
			// pass
		}
	}

	@Test
	public void setTopKOverridesPriorReduce() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 5));
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(3);

		// setTopK() replaces any previously set reduce, same as setReduce().
		assertTrue(stmt.resolveReduce() instanceof ReduceSpec);
		stmt.resolveReduce().acceptPartial(record(BIN, 1L), key("k1"));
		assertEquals(1, stmt.resolveReduce().getResult().length);
	}

	//-------------------------------------------------------
	// Top-K: additional permutations
	//-------------------------------------------------------

	@Test
	public void topKWithKOne() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 1);

		for (int i = 1; i <= 5; i++) {
			reducer.acceptPartial(record(BIN, (long)i), key("k" + i));
		}

		Record[] result = reducer.getResult();
		assertEquals(1, result.length);
		assertEquals(5L, result[0].getLong(BIN));
		assertSame(result[0], reducer.getScalarResult());
	}

	@Test
	public void topKUpperBoundKThousandAccepted() {
		Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 1000); // must not throw
	}

	@Test
	public void topKScalarResultBeforeAnyAcceptThrows() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

		try {
			reducer.getScalarResult();
			fail("Expected IllegalStateException");
		}
		catch (IllegalStateException expected) {
			// pass
		}
	}

	@Test
	public void topKDescTiesOrderedByDigestAscending() {
		// All values equal; tie-break is digest-ascending and independent of Order direction.
		ReduceSpec<Record, Record> asc = Reduce.topK(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE, 3);
		ReduceSpec<Record, Record> desc = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 3);

		String[] userKeys = {"alpha", "beta", "gamma", "delta", "epsilon"};

		// Feed the exact same (key, record) instances to both reducers so identical selection
		// and ordering can be asserted by reference.
		for (String uk : userKeys) {
			Record shared = record(BIN, 5L);
			asc.acceptPartial(shared, key(uk));
			desc.acceptPartial(shared, key(uk));
		}

		Record[] ra = asc.getResult();
		Record[] rd = desc.getResult();
		assertEquals(3, ra.length);
		assertEquals(3, rd.length);

		// Both orderings pick the same 3 records (the 3 with smallest digests) in the same order.
		for (int i = 0; i < ra.length; i++) {
			assertSame(ra[i], rd[i]);
		}
	}

	@Test
	public void topKBytesOrdering() {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.BYTES, Order.ASC, OrderByFlags.NONE, 2);

		reducer.acceptPartial(record(BIN, new byte[] {0x01, 0x02}), key("k1"));
		reducer.acceptPartial(record(BIN, new byte[] {0x01}), key("k2"));
		reducer.acceptPartial(record(BIN, new byte[] {(byte)0xff}), key("k3"));

		Record[] result = reducer.getResult();
		assertEquals(2, result.length);
		// {0x01} is a prefix of {0x01,0x02} so it sorts first; 0xff is unsigned-largest.
		assertEquals(1, result[0].getBytes(BIN).length);
		assertEquals(2, result[1].getBytes(BIN).length);
	}

	@Test
	public void topKCaseSensitiveStringOrdering() {
		// Without CASE_INSENSITIVE, uppercase sorts before lowercase (ASCII order).
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.STRING, Order.ASC, OrderByFlags.NONE, 3);

		reducer.acceptPartial(record(BIN, "banana"), key("k1"));
		reducer.acceptPartial(record(BIN, "Apple"), key("k2"));
		reducer.acceptPartial(record(BIN, "Cherry"), key("k3"));

		Record[] result = reducer.getResult();
		assertEquals("Apple", result[0].getString(BIN));
		assertEquals("Cherry", result[1].getString(BIN));
		assertEquals("banana", result[2].getString(BIN));
	}

	//-------------------------------------------------------
	// Statement: additional permutations
	//-------------------------------------------------------

	@Test
	public void statementResolveReduceDuplicateOrderByThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE),
			Reduce.limit(BIN, 2));

		try {
			stmt.resolveReduce();
			fail("Expected IllegalArgumentException for duplicate orderBy");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void statementResolveReduceDuplicateLimitThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.limit(BIN, 2),
			Reduce.limit(BIN, 3));

		try {
			stmt.resolveReduce();
			fail("Expected IllegalArgumentException for duplicate limit");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void setReduceEmptyClearsReduce() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));
		assertTrue(stmt.resolveReduce() != null);

		stmt.setReduce(); // empty varargs
		assertNull(stmt.resolveReduce());
	}

	//-------------------------------------------------------
	// Concurrency: combiners are fed from one thread per node concurrently
	//-------------------------------------------------------

	@Test
	public void topKIsThreadSafeUnderConcurrentAccept() throws Exception {
		ReduceSpec<Record, Record> reducer = Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 5);
		int threads = 8;
		int perThread = 20000;
		long total = (long)threads * perThread;

		// Each thread produces a disjoint, globally-unique value range so the overall top 5 are
		// distinct: total-1, total-2, ..., total-5.
		runConcurrent(threads, (t) -> {
			for (int i = 0; i < perThread; i++) {
				long val = (long)t * perThread + i;
				reducer.acceptPartial(record(BIN, val), key("t" + t + "-k" + i));
			}
		});

		Record[] result = reducer.getResult();
		assertEquals(5, result.length);
		for (int i = 0; i < 5; i++) {
			assertEquals(total - 1 - i, result[i].getLong(BIN));
		}
	}

	private interface ThreadBody {
		void run(int threadIndex);
	}

	private static void runConcurrent(int threadCount, ThreadBody body) throws Exception {
		CyclicBarrier barrier = new CyclicBarrier(threadCount);
		AtomicReference<Throwable> failure = new AtomicReference<>();
		List<Thread> threads = new ArrayList<>();

		for (int t = 0; t < threadCount; t++) {
			final int idx = t;
			Thread thread = new Thread(() -> {
				try {
					barrier.await();
					body.run(idx);
				}
				catch (Throwable e) {
					failure.compareAndSet(null, e);
				}
			});
			threads.add(thread);
			thread.start();
		}

		for (Thread thread : threads) {
			thread.join();
		}

		Throwable t = failure.get();

		if (t != null) {
			throw new AssertionError("Concurrent acceptPartial threw: " + t, t);
		}
	}
}
