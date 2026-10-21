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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.QueryPolicy;

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
	private static final String GROUP = "group";
	private static final String MEMBER = "member";

	private static Key key(String userKey) {
		return new Key(NS, SET, userKey);
	}

	private static TopKReduceSpec topK(int k) {
		return new TopKReduceSpec(
			new OrderBy[] {new OrderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE)}, k);
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
	public void resultKeyRecordsPairKeysWithRecordsInRankOrder() {
		TopKReduceSpec topK = topK(3);

		for (int i = 1; i <= 5; i++) {
			topK.acceptPartial(record(BIN, (long)i), key("k" + i));
		}

		KeyRecord[] result = topK.getResultKeyRecords();
		assertEquals(3, result.length);

		for (int i = 0; i < result.length; i++) {
			long value = 5 - i;
			assertEquals(value, result[i].record.getLong(BIN));
			assertEquals(key("k" + value), result[i].key);
		}
	}

	@Test
	public void fullHeapAcceptsBetterRescanOfRejectedRecord() {
		TopKReduceSpec topK = topK(2);
		topK.acceptPartial(record(BIN, 10L), key("k10"));
		topK.acceptPartial(record(BIN, 20L), key("k20"));
		topK.acceptPartial(record(BIN, 5L), key("k5"));
		topK.acceptPartial(record(BIN, 30L), key("k5"));

		KeyRecord[] result = topK.getResultKeyRecords();
		assertEquals(2, result.length);
		assertEquals(key("k5"), result[0].key);
		assertEquals(30L, result[0].record.getLong(BIN));
		assertEquals(key("k20"), result[1].key);
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
	public void resolveReduceReturnsEmptyCombinerEachRun() {
		Statement pushdown = new Statement();
		pushdown.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);
		pushdown.setTopK(2);

		Statement clientOnly = new Statement();
		clientOnly.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));

		Statement split = new Statement();
		split.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.limit(BIN, 2));

		for (Statement stmt : new Statement[] {pushdown, clientOnly, split}) {
			ReduceSpec<Record, Record> first = stmt.resolveReduce();
			first.acceptPartial(record(BIN, 1L), key("k1"));

			// A statement that runs again must not rank the records of an earlier run.
			assertEquals(0, stmt.<Record, Record>resolveReduce().getResult().length);
			assertEquals(1, first.getResult().length);
		}
	}

	@Test
	public void statementResolveReduceOrderByWithoutLimitThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE));

		assertParameterError(stmt::resolveReduce);
	}

	@Test
	public void statementResolveReduceComposeBinMismatchThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy("binA", BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.limit("binB", 2));

		assertParameterError(stmt::resolveReduce);
	}

	@Test
	public void statementResolveReduceDuplicateOrderByThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.NONE),
			Reduce.limit(BIN, 2));

		assertParameterError(stmt::resolveReduce);
	}

	@Test
	public void statementResolveReduceDuplicateLimitThrows() {
		Statement stmt = new Statement();
		stmt.setReduce(
			Reduce.orderBy(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE),
			Reduce.limit(BIN, 2),
			Reduce.limit(BIN, 3));

		assertParameterError(stmt::resolveReduce);
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
		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();

		for (int i = 1; i <= 5; i++) {
			resolved.acceptPartial(record(BIN, (long)i), key("k" + i));
		}

		Record[] result = resolved.getResult();
		assertEquals(3, result.length);
		assertEquals(5L, result[0].getLong(BIN));
	}

	@Test
	public void topKRejectsInvalidWireSpecification() {
		Statement flags = new Statement();
		flags.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC, OrderByFlags.CASE_INSENSITIVE);
		flags.setTopK(1);
		assertInvalidTopK(flags);

		Statement projection = new Statement();
		projection.setBinNames("other");
		projection.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		projection.setTopK(1);
		assertInvalidTopK(projection);

		Statement name = new Statement();
		name.setOrderBy("sixteen-byte-bin", BinDataType.INTEGER, Order.ASC);
		name.setTopK(1);
		assertInvalidTopK(name);

		Statement limit = new Statement();
		limit.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		limit.setTopK(0);
		assertInvalidTopK(limit);

		Statement noOrderKey = new Statement();
		noOrderKey.setTopK(5);
		assertInvalidTopK(noOrderKey);
		assertParameterError(noOrderKey::resolveReduce);
	}

	@Test
	public void setTopKBeforeSetOrderByResolvesToTopK() {
		Statement stmt = new Statement();
		stmt.setTopK(2);
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.validateTopK();

		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();
		resolved.acceptPartial(record(BIN, 3L), key("k1"));
		resolved.acceptPartial(record(BIN, 1L), key("k2"));
		resolved.acceptPartial(record(BIN, 2L), key("k3"));

		Record[] result = resolved.getResult();
		assertEquals(2, result.length);
		assertEquals(1L, result[0].getLong(BIN));
		assertEquals(2L, result[1].getLong(BIN));
	}

	@Test
	public void orderKeyWithoutKIsKept() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);

		assertFalse(stmt.hasTopK());
		assertTrue(stmt.hasOrderBy());
		assertEquals(BIN, stmt.getOrderBy()[0].binName);
	}

	@Test
	public void clientOnlyReduceClearsOrderKeys() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(2);
		stmt.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, 2));

		assertFalse(stmt.hasTopK());
		assertFalse(stmt.hasOrderBy());
		assertNull(stmt.getOrderBy());
	}

	@Test
	public void repeatedSettersReplaceTopKParts() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(5);
		stmt.resolveReduce();

		stmt.setTopK(3);
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);

		assertEquals(3, stmt.getTopKLimit());
		assertEquals(Order.DESC, stmt.getOrderBy()[0].order);

		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();

		for (int i = 1; i <= 5; i++) {
			resolved.acceptPartial(record(BIN, (long)i), key("k" + i));
		}

		Record[] result = resolved.getResult();
		assertEquals(3, result.length);
		assertEquals(5L, result[0].getLong(BIN));
	}

	@Test
	public void topKRejectsOrderKeyFromMultipleProjectionOperations() {
		Statement stmt = new Statement();
		stmt.setOperations(new Operation[] {Operation.get(BIN), Operation.get(BIN)});
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);
		assertInvalidTopK(stmt);
	}

	@Test
	public void topKRejectsResumedPartitionFilter() {
		Statement stmt = new Statement();
		stmt.setNamespace(NS);
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);

		PartitionFilter after = PartitionFilter.after(key("k1"));
		assertParameterError(() -> new PartitionTracker(new QueryPolicy(), stmt, new Node[0], after));
		assertNull(after.getPartitions());

		PartitionFilter cursor = PartitionFilter.range(0, 2);
		PartitionStatus resumed = new PartitionStatus(1);
		resumed.digest = key("k1").digest;
		cursor.setPartitions(new PartitionStatus[] {new PartitionStatus(0), resumed});
		assertParameterError(() -> new PartitionTracker(new QueryPolicy(), stmt, 1, cursor));
	}

	@Test
	public void topKAcceptsFreshPartitionFilter() {
		Statement stmt = new Statement();
		stmt.setNamespace(NS);
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);

		new PartitionTracker(new QueryPolicy(), stmt, 1, PartitionFilter.range(0, 2));
	}

	//-------------------------------------------------------
	// Second order key (thenOrderBy)
	//-------------------------------------------------------

	@Test
	public void secondKeyDecidesOnlyWhereFirstTies() {
		Statement stmt = new Statement();
		stmt.setOrderBy(GROUP, BinDataType.INTEGER, Order.DESC);
		stmt.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(3);
		stmt.validateTopK();

		Record[] result = rank(stmt,
			record2(2L, 5L), record2(1L, 0L), record2(2L, 1L), record2(2L, 3L));

		assertEquals(3, result.length);
		assertEquals(1L, result[0].getLong(MEMBER));
		assertEquals(3L, result[1].getLong(MEMBER));
		assertEquals(5L, result[2].getLong(MEMBER));
	}

	@Test
	public void secondKeyRanksRecordsNilOnFirstKey() {
		Statement stmt = new Statement();
		stmt.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		stmt.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(4);

		// A missing or mismatched first key is NIL: it ranks last, then by the second key.
		Record[] result = rank(stmt,
			record2(null, 2L), record2(1L, 9L), record2("one", 0L), record2(null, 1L));

		assertEquals(4, result.length);
		assertEquals(9L, result[0].getLong(MEMBER));
		assertEquals(0L, result[1].getLong(MEMBER));
		assertEquals(1L, result[2].getLong(MEMBER));
		assertEquals(2L, result[3].getLong(MEMBER));
	}

	@Test
	public void setOrderByReplacesBothKeysAndKeepsK() {
		Statement stmt = new Statement();
		stmt.setOrderBy(GROUP, BinDataType.INTEGER, Order.DESC);
		stmt.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(2);
		stmt.setOrderBy(MEMBER, BinDataType.INTEGER, Order.DESC);

		OrderBy[] keys = stmt.getOrderBy();
		assertEquals(1, keys.length);
		assertEquals(MEMBER, keys[0].binName);
		assertEquals(Order.DESC, keys[0].order);
		assertEquals(2, stmt.getTopKLimit());
	}

	@Test
	public void thenOrderByBeforeSetOrderByAddsFirstKey() {
		Statement stmt = new Statement();
		stmt.thenOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);
		stmt.validateTopK();

		OrderBy[] keys = stmt.getOrderBy();
		assertEquals(1, keys.length);
		assertEquals(GROUP, keys[0].binName);
	}

	@Test
	public void topKRejectsThirdOrderKey() {
		Statement stmt = new Statement();
		stmt.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		stmt.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		stmt.thenOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);
		assertInvalidTopK(stmt);
	}

	@Test
	public void topKRejectsRepeatedOrderKeyBin() {
		Statement stmt = new Statement();
		stmt.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		stmt.thenOrderBy(GROUP, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(1);
		assertInvalidTopK(stmt);
	}

	@Test
	public void topKValidatesSecondKeyProjection() {
		Statement binList = new Statement();
		binList.setBinNames(GROUP);
		binList.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		binList.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		binList.setTopK(1);
		assertInvalidTopK(binList);

		Statement operations = new Statement();
		operations.setOperations(new Operation[] {
			Operation.get(GROUP), Operation.get(MEMBER), Operation.get(MEMBER)});
		operations.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		operations.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		operations.setTopK(1);
		assertInvalidTopK(operations);
	}

	private static Record record2(Object group, Object member) {
		Map<String, Object> bins = new HashMap<>();

		if (group != null) {
			bins.put(GROUP, group);
		}
		bins.put(MEMBER, member);
		return new Record(bins, 1, 0);
	}

	private static Record[] rank(Statement stmt, Record... records) {
		ReduceSpec<Record, Record> resolved = stmt.resolveReduce();

		for (int i = 0; i < records.length; i++) {
			resolved.acceptPartial(records[i], key("k" + i));
		}
		return resolved.getResult();
	}

	private static void assertInvalidTopK(Statement statement) {
		assertParameterError(statement::validateTopK);
	}

	private static void assertParameterError(Runnable action) {
		try {
			action.run();
			fail("Expected AerospikeException");
		}
		catch (AerospikeException expected) {
			assertEquals(ResultCode.PARAMETER_ERROR, expected.getResultCode());
		}
	}
}
