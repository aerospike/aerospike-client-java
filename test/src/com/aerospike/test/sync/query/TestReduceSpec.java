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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

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
	// Scalar reducers: sum / count / min / max
	//-------------------------------------------------------

	@Test
	public void sumIsCommutativeAndAssociative() {
		ReduceSpec<Record, Long> reducer = Reduce.sum(BIN);

		// Feed out of order, simulating partials arriving from different nodes.
		reducer.acceptPartial(record(BIN, 5L), key("k3"));
		reducer.acceptPartial(record(BIN, 10L), key("k1"));
		reducer.acceptPartial(record(BIN, 7L), key("k2"));

		assertEquals(22L, reducer.getScalarResult().longValue());
		assertEquals(1, reducer.getResult().length);
		assertEquals(22L, reducer.getResult()[0].longValue());
	}

	@Test
	public void countIncrementsOncePerAccept() {
		ReduceSpec<Record, Long> reducer = Reduce.count();

		for (int i = 0; i < 7; i++) {
			reducer.acceptPartial(record(BIN, i), key("k" + i));
		}
		assertEquals(7L, reducer.getScalarResult().longValue());
	}

	@Test
	public void minPicksSmallestInteger() {
		ReduceSpec<Record, Number> reducer = Reduce.min(BIN, BinDataType.INTEGER);

		reducer.acceptPartial(record(BIN, 9L), key("k1"));
		reducer.acceptPartial(record(BIN, 2L), key("k2"));
		reducer.acceptPartial(record(BIN, 5L), key("k3"));

		assertEquals(2L, reducer.getScalarResult().longValue());
	}

	@Test
	public void maxPicksLargestDouble() {
		ReduceSpec<Record, Number> reducer = Reduce.max(BIN, BinDataType.DOUBLE);

		reducer.acceptPartial(record(BIN, 1.5), key("k1"));
		reducer.acceptPartial(record(BIN, 9.25), key("k2"));
		reducer.acceptPartial(record(BIN, 4.0), key("k3"));

		assertEquals(9.25, reducer.getScalarResult().doubleValue(), 0.0001);
	}

	@Test
	public void minMaxRejectsNonNumericBinDataType() {
		try {
			Reduce.min(BIN, BinDataType.STRING);
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void minMaxOrderIndependent() {
		// Merge order should not affect the result (commutative).
		ReduceSpec<Record, Number> a = Reduce.min(BIN, BinDataType.INTEGER);
		a.acceptPartial(record(BIN, 3L), key("k1"));
		a.acceptPartial(record(BIN, 1L), key("k2"));
		a.acceptPartial(record(BIN, 2L), key("k3"));

		ReduceSpec<Record, Number> b = Reduce.min(BIN, BinDataType.INTEGER);
		b.acceptPartial(record(BIN, 2L), key("k3"));
		b.acceptPartial(record(BIN, 1L), key("k2"));
		b.acceptPartial(record(BIN, 3L), key("k1"));

		assertEquals(a.getScalarResult(), b.getScalarResult());
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
	public void statementResolvesSingleScalarReduce() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.sum(BIN));

		ReduceSpec<Record, Long> resolved = stmt.resolveReduce();
		resolved.acceptPartial(record(BIN, 4L), key("k1"));
		assertEquals(4L, resolved.getScalarResult().longValue());
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
		stmt.setReduce(Reduce.sum(BIN));
		ReduceSpec<Record, Long> first = stmt.resolveReduce();

		stmt.setReduce(Reduce.count());
		ReduceSpec<Record, Long> second = stmt.resolveReduce();

		assertTrue(first != second);
	}

	@Test
	public void statementResolveReduceMixingTopKPartsWithScalarThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.sum(BIN));

		try {
			stmt.resolveReduce();
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
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
		stmt.setReduce(Reduce.sum(BIN));
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(3);

		// setTopK() replaces any previously set reduce, same as setReduce().
		assertTrue(stmt.resolveReduce() instanceof ReduceSpec);
		stmt.resolveReduce().acceptPartial(record(BIN, 1L), key("k1"));
		assertEquals(1, stmt.resolveReduce().getResult().length);
	}

	//-------------------------------------------------------
	// Statement validation used by the query executor entry points
	//-------------------------------------------------------

	@Test
	public void validateRecordQueryAllowsNoReduce() {
		new Statement().validateRecordQuery(); // should not throw
	}

	@Test
	public void validateRecordQueryAllowsTopK() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));
		stmt.validateRecordQuery(); // should not throw
	}

	@Test
	public void validateRecordQueryRejectsScalarReduce() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.sum(BIN));

		try {
			stmt.validateRecordQuery();
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void validateReduceQueryRejectsNoReduce() {
		try {
			new Statement().validateReduceQuery();
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void validateReduceQueryRejectsTopK() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));

		try {
			stmt.validateReduceQuery();
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalArgumentException expected) {
			// pass
		}
	}

	@Test
	public void validateReduceQueryAllowsScalarReduce() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.count());
		stmt.validateReduceQuery(); // should not throw
	}
}
