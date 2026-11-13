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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.QueryPolicy;

/**
 * White-box unit tests for the public {@link Statement#setOrderBy} / {@link Statement#setTopK}
 * setters, checked through the package-private {@code Statement.createTopK} merge they produce.
 * Pure client-side (no cluster required); registered in {@code SuiteUnit}.
 */
public class TestStatementReduce {
	private static final String NS = "test";
	private static final String SET = "reduceset";
	private static final String BIN = "val";
	private static final String GROUP = "group";
	private static final String MEMBER = "member";
	private static final String BIN_NAME = "Top-K order-by bin name must be 1-15 UTF-8 bytes without NUL";

	private static Key key(String userKey) {
		return new Key(NS, SET, userKey);
	}

	private static TopKReduceSpec topK(int k) {
		return new TopKReduceSpec(
			new OrderBy[] {new OrderBy(BIN, BinDataType.INTEGER, Order.DESC)}, k);
	}

	private static Record record(String binName, Object value) {
		Map<String, Object> bins = new HashMap<>();
		bins.put(binName, value);
		return new Record(bins, 1, 0);
	}

	@Test
	public void statementDefaultsToNoTopK() {
		Statement stmt = new Statement();
		assertNull(stmt.createTopK());
	}

	@Test
	public void resultKeyRecordsPairKeysWithRecordsInRankOrder() {
		TopKReduceSpec topK = topK(3);

		for (int i = 1; i <= 5; i++) {
			topK.acceptPartial(record(BIN, (long)i), key("k" + i), 0);
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
		topK.acceptPartial(record(BIN, 10L), key("k10"), 0);
		topK.acceptPartial(record(BIN, 20L), key("k20"), 0);
		topK.acceptPartial(record(BIN, 5L), key("k5"), 0);
		topK.acceptPartial(record(BIN, 30L), key("k5"), 0);

		KeyRecord[] result = topK.getResultKeyRecords();
		assertEquals(2, result.length);
		assertEquals(key("k5"), result[0].key);
		assertEquals(30L, result[0].record.getLong(BIN));
		assertEquals(key("k20"), result[1].key);
	}

	@Test
	public void createTopKReturnsEmptyCombinerEachRun() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(2);

		TopKReduceSpec first = stmt.createTopK();
		first.acceptPartial(record(BIN, 1L), key("k1"), 0);

		// A statement that runs again must not rank the records of an earlier run.
		assertEquals(0, stmt.createTopK().getResultKeyRecords().length);
		assertEquals(1, first.getResultKeyRecords().length);
	}

	//-------------------------------------------------------
	// Statement.setOrderBy / setTopK (public API), verified via the package-private createTopK()
	//-------------------------------------------------------

	@Test
	public void setOrderByThenSetTopKResolvesToTopK() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.DOUBLE, Order.ASC);
		stmt.setTopK(2);

		TopKReduceSpec resolved = stmt.createTopK();
		resolved.acceptPartial(record(BIN, 3.0), key("k1"), 0);
		resolved.acceptPartial(record(BIN, 1.0), key("k2"), 0);
		resolved.acceptPartial(record(BIN, 2.0), key("k3"), 0);

		Record[] result = records(resolved);
		assertEquals(2, result.length);
		assertEquals(1.0, result[0].getDouble(BIN), 0.0001);
		assertEquals(2.0, result[1].getDouble(BIN), 0.0001);
	}

	@Test
	public void topKRejectsInvalidWireSpecification() {
		Statement projection = new Statement();
		projection.setBinNames("other");
		projection.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		projection.setTopK(1);
		assertInvalidTopK(projection,
			"orderBy bin 'val' is not in projection; add it to the projection or remove the projection");

		Statement name = new Statement();
		name.setOrderBy("sixteen-byte-bin", BinDataType.INTEGER, Order.ASC);
		name.setTopK(1);
		assertInvalidTopK(name, BIN_NAME);

		Statement noName = new Statement();
		noName.setOrderBy(null, BinDataType.INTEGER, Order.ASC);
		noName.setTopK(1);
		assertInvalidTopK(noName, BIN_NAME);

		Statement limit = new Statement();
		limit.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		limit.setTopK(0);
		assertInvalidTopK(limit, "topK must be in [1, 1000]; got 0");

		limit.setTopK(1001);
		assertInvalidTopK(limit, "topK must be in [1, 1000]; got 1001");

		limit.setTopK(1000);
		limit.validateTopK();

		Statement noOrderKey = new Statement();
		noOrderKey.setTopK(5);
		assertInvalidTopK(noOrderKey, "Top-K requires an order key; call setOrderBy()");
		assertInvalidArgument(noOrderKey::createTopK);
	}

	@Test
	public void topKRejectsOrderKeyWithoutTypeOrDirection() {
		Statement type = new Statement();
		type.setOrderBy(BIN, null, Order.ASC);
		type.setTopK(1);
		assertInvalidTopK(type,
			"orderBy requires a type declaration (INTEGER, DOUBLE or BYTES); Aerospike has no schema");

		Statement direction = new Statement();
		direction.setOrderBy(BIN, BinDataType.INTEGER, null);
		direction.setTopK(1);
		assertInvalidTopK(direction, "orderBy direction must be ASC or DESC");
	}

	@Test
	public void topKRejectsAggregateFunction() {
		Statement stmt = new Statement();
		stmt.setAggregateFunction("sum_example", "sum_single_bin");
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);
		assertInvalidTopK(stmt,
			"orderBy is incompatible with aggregate UDFs; use the UDF's own reduce stage");
	}

	@Test
	public void setTopKBeforeSetOrderByResolvesToTopK() {
		Statement stmt = new Statement();
		stmt.setTopK(2);
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.validateTopK();

		TopKReduceSpec resolved = stmt.createTopK();
		resolved.acceptPartial(record(BIN, 3L), key("k1"), 0);
		resolved.acceptPartial(record(BIN, 1L), key("k2"), 0);
		resolved.acceptPartial(record(BIN, 2L), key("k3"), 0);

		Record[] result = records(resolved);
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
	public void repeatedSettersReplaceTopKParts() {
		Statement stmt = new Statement();
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(5);
		stmt.createTopK();

		stmt.setTopK(3);
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);

		assertEquals(3, stmt.getTopKLimit());
		assertEquals(Order.DESC, stmt.getOrderBy()[0].order);

		TopKReduceSpec resolved = stmt.createTopK();

		for (int i = 1; i <= 5; i++) {
			resolved.acceptPartial(record(BIN, (long)i), key("k" + i), 0);
		}

		Record[] result = records(resolved);
		assertEquals(3, result.length);
		assertEquals(5L, result[0].getLong(BIN));
	}

	@Test
	public void topKRejectsOrderKeyFromMultipleProjectionOperations() {
		Statement stmt = new Statement();
		stmt.setOperations(new Operation[] {Operation.get(BIN), Operation.get(BIN)});
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);
		assertInvalidTopK(stmt, "orderBy bin 'val' is produced by more than one operation");
	}

	@Test
	public void topKRejectsResumedPartitionFilter() {
		Statement stmt = new Statement();
		stmt.setNamespace(NS);
		stmt.setOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);

		PartitionFilter after = PartitionFilter.after(key("k1"));
		AerospikeException ae = assertInvalidArgument(
			() -> new PartitionTracker(new QueryPolicy(), stmt, new Node[0], after));
		assertEquals("Top-K can't resume a partition filter from an earlier query", ae.getBaseMessage());
		assertNull(after.getPartitions());

		PartitionFilter cursor = PartitionFilter.range(0, 2);
		PartitionStatus resumed = new PartitionStatus(1);
		resumed.digest = key("k1").digest;
		cursor.setPartitions(new PartitionStatus[] {new PartitionStatus(0), resumed});
		assertInvalidArgument(() -> new PartitionTracker(new QueryPolicy(), stmt, 1, cursor));
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
	public void thenOrderByBeforeSetOrderByIsRejected() {
		Statement stmt = new Statement();
		stmt.setTopK(1);
		AerospikeException ae = assertInvalidArgument(
			() -> stmt.thenOrderBy(GROUP, BinDataType.INTEGER, Order.ASC));
		assertEquals("thenOrderBy requires a first order key; call setOrderBy() first",
			ae.getBaseMessage());
		assertNull(stmt.getOrderBy());
	}

	@Test
	public void topKRejectsThirdOrderKey() {
		Statement stmt = new Statement();
		stmt.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		stmt.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		stmt.thenOrderBy(BIN, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(1);
		assertInvalidTopK(stmt, "orderBy supports at most two keys");
	}

	@Test
	public void topKRejectsRepeatedOrderKeyBin() {
		Statement stmt = new Statement();
		stmt.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		stmt.thenOrderBy(GROUP, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(1);
		assertInvalidTopK(stmt, "orderBy keys must use different bins");
	}

	@Test
	public void topKValidatesSecondKeyProjection() {
		Statement binList = new Statement();
		binList.setBinNames(GROUP);
		binList.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		binList.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		binList.setTopK(1);
		assertInvalidTopK(binList, "orderBy bin 'member' is not in projection; " +
			"add it to the projection or remove the projection");

		Statement operations = new Statement();
		operations.setOperations(new Operation[] {
			Operation.get(GROUP), Operation.get(MEMBER), Operation.get(MEMBER)});
		operations.setOrderBy(GROUP, BinDataType.INTEGER, Order.ASC);
		operations.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		operations.setTopK(1);
		assertInvalidTopK(operations, "orderBy bin 'member' is produced by more than one operation");
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
		TopKReduceSpec resolved = stmt.createTopK();

		for (int i = 0; i < records.length; i++) {
			resolved.acceptPartial(records[i], key("k" + i), 0);
		}
		return records(resolved);
	}

	private static Record[] records(TopKReduceSpec topK) {
		KeyRecord[] keyRecords = topK.getResultKeyRecords();
		Record[] records = new Record[keyRecords.length];

		for (int i = 0; i < keyRecords.length; i++) {
			records[i] = keyRecords[i].record;
		}
		return records;
	}

	private static void assertInvalidTopK(Statement statement, String message) {
		assertEquals(message, assertInvalidArgument(statement::validateTopK).getBaseMessage());
	}

	private static AerospikeException assertInvalidArgument(ThrowingRunnable action) {
		AerospikeException ae = assertThrows(AerospikeException.InvalidArgument.class, action);
		assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
		return ae;
	}
}
