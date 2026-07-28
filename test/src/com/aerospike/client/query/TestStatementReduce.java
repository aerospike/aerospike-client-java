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
package com.aerospike.client.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * White-box unit tests for the package-private {@code Statement.setReduce} / {@code getReduce} /
 * {@code resolveReduce} internals that back the public {@link Statement#setOrderBy} /
 * {@link Statement#setTopK} sugar. These are not reachable from outside
 * {@code com.aerospike.client.query} since {@code setReduce} is intentionally not part of the
 * public API for now. Pure client-side (no cluster required); registered in {@code SuiteUnit}.
 */
public class TestStatementReduce {
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
	// Statement.setOrderBy / setTopK sugar (public API), verified via the
	// package-private resolveReduce()
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
}
