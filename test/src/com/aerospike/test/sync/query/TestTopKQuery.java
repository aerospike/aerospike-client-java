/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.test.sync.query;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.PartitionStatus;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

/**
 * Verifies Top-K results against a live cluster: server-side ranking on each node plus a client
 * merge ({@link Statement#setOrderBy} + {@link Statement#setTopK}) must produce the global Top-K.
 * Also checked with each order-key type, filters, k bounds, node concurrency, the record queue,
 * timeouts, partition ranges, and single-node queries. The wire encoding of the request is
 * covered separately by {@code TestTopKRequestEncoding}.
 */
public class TestTopKQuery extends TestSync {
	private static final String SET = "topKQuery";
	private static final String EMPTY_SET = "topKQueryEmpty";
	private static final String INDEX = "topKQueryGroup";
	private static final String BIN = "rank";
	private static final String DERIVED = "derived";
	private static final String GROUP = "group";
	private static final String MEMBER = "member";
	private static final String BLOB = "blob";
	private static final int COUNT = 100;

	@BeforeClass
	public static void prepare() {
		for (int i = 1; i <= COUNT; i++) {
			// First bytes run from 0x65 to 0xc8, across 0x7f/0x80, where a signed comparison
			// inverts the order.
			byte[] blob = {(byte)(i + 100), 0};

			// Ten groups of ten records each, so a second order key has ties to break.
			client.put(null, key(i), new Bin(BIN, i), new Bin(GROUP, (i - 1) / 10),
				new Bin(MEMBER, (i - 1) % 10), new Bin(BLOB, blob));
		}

		try {
			IndexTask task = client.createIndex(args.indexPolicy, args.namespace, SET, INDEX, GROUP,
				IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, SET, INDEX);

		for (int i = 1; i <= COUNT; i++) {
			client.delete(null, key(i));
		}
	}

	private static void requireCapableCluster() {
		Assume.assumeTrue("Server-side Top-K requires server version 8.2.1 or later on every node",
			client.getCluster().hasQueryOrderBy);
	}

	@Test
	public void topKFailsWhenAnyNodeLacksSupport() {
		Cluster cluster = client.getCluster();
		boolean saved = cluster.hasQueryOrderBy;
		cluster.hasQueryOrderBy = false;

		try {
			String message = "orderBy/topK is not supported by all nodes in the cluster";
			AerospikeException ae = assertThrows(AerospikeException.InvalidArgument.class,
				() -> client.query(null, ranked(Order.DESC, 3)));
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertEquals(message, ae.getBaseMessage());

			ae = assertThrows(AerospikeException.InvalidArgument.class,
				() -> client.query(null, orderKeyOnly()));
			assertEquals(message, ae.getBaseMessage());
		}
		finally {
			cluster.hasQueryOrderBy = saved;
		}
	}

	@Test
	public void topKLeavesNoResumeCursor() {
		requireCapableCluster();

		PartitionFilter topKFilter = PartitionFilter.all();
		drain(client.queryPartitions(null, ranked(Order.DESC, 3), topKFilter));

		for (PartitionStatus ps : topKFilter.getPartitions()) {
			assertNull(ps.digest);
		}

		PartitionFilter plainFilter = PartitionFilter.all();
		drain(client.queryPartitions(null, base(BIN), plainFilter));

		boolean anyCursor = false;

		for (PartitionStatus ps : plainFilter.getPartitions()) {
			anyCursor |= ps.digest != null;
		}
		assertTrue(anyCursor);
	}

	@Test
	public void orderKeyWithoutKIsRejectedByServer() {
		requireCapableCluster();

		QueryPolicy policy = new QueryPolicy();
		policy.errorDetailVerbosity = 2;

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> values(client.query(policy, orderKeyOnly()), BIN));
		assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
		assertFalse(ae instanceof AerospikeException.InvalidArgument);
		// The subcode is not asserted because its number is not final.
		assertTrue("Expected the server's reason, got: " + ae.getBaseMessage(),
			ae.getBaseMessage().contains("top-k needs both order-by and top-k fields"));
	}

	@Test
	public void returnsGlobalTopK() {
		requireCapableCluster();

		for (int k : new int[] {1, 3, 5, 25}) {
			assertArrayEquals("ASC k=" + k, ascending(1, k), values(ranked(Order.ASC, k), BIN));
			assertArrayEquals("DESC k=" + k, descending(COUNT, k), values(ranked(Order.DESC, k), BIN));
		}
	}

	@Test
	public void expressionProjectedKeyRanks() {
		requireCapableCluster();

		int k = 5;
		long[] expected = new long[k];

		for (int i = 0; i < k; i++) {
			expected[i] = (i + 1) * 10L;
		}

		assertArrayEquals(expected, values(rankedDerived(Order.ASC, k), DERIVED));
	}

	@Test
	public void secondOrderKeyBreaksTies() {
		requireCapableCluster();

		Statement statement = base(BIN);
		statement.setBinNames(BIN, GROUP, MEMBER);
		statement.setOrderBy(GROUP, BinDataType.INTEGER, Order.DESC);
		statement.thenOrderBy(MEMBER, BinDataType.INTEGER, Order.ASC);
		statement.setTopK(15);

		// The whole top group by member, then the next group's five smallest members.
		long[] expected = {91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 81, 82, 83, 84, 85};
		assertArrayEquals(expected, values(statement, BIN));
	}

	@Test
	public void missingOrderKeyRanksLast() {
		requireCapableCluster();

		Key[] missing = {
			new Key(args.namespace, SET, "missing1"),
			new Key(args.namespace, SET, "missing2")
		};

		for (Key key : missing) {
			client.put(null, key, new Bin(GROUP, -1));
		}

		try {
			int k = COUNT + missing.length;

			for (Order order : new Order[] {Order.ASC, Order.DESC}) {
				Statement byBinName = ranked(order, k);
				Statement byOperation = ranked(order, k);
				byOperation.setBinNames((String[])null);
				byOperation.setOperations(new Operation[] {Operation.get(BIN)});

				for (Statement statement : new Statement[] {byBinName, byOperation}) {
					List<Object> ranked = rankedValues(statement, BIN);
					assertEquals(k, ranked.size());

					for (int i = 0; i < COUNT; i++) {
						long expected = order == Order.ASC ? i + 1 : COUNT - i;
						assertEquals(expected, ranked.get(i));
					}
					assertNull(ranked.get(COUNT));
					assertNull(ranked.get(COUNT + 1));
				}
			}
		}
		finally {
			for (Key key : missing) {
				client.delete(null, key);
			}
		}
	}

	@Test
	public void everyNodeConcurrencyReturnsGlobalTopK() {
		requireCapableCluster();

		// The serial (1) and thread-chaining (2) paths need a cluster of at least three nodes.
		for (int maxConcurrentNodes : new int[] {0, 1, 2}) {
			QueryPolicy policy = new QueryPolicy();
			policy.maxConcurrentNodes = maxConcurrentNodes;

			assertArrayEquals("maxConcurrentNodes=" + maxConcurrentNodes, descending(COUNT, 10),
				values(client.query(policy, topK(BIN, BinDataType.INTEGER, Order.DESC, 10)), BIN));
		}
	}

	@Test
	public void filterExpressionLimitsCandidates() {
		requireCapableCluster();

		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.mod(Exp.intBin(BIN), Exp.val(2)), Exp.val(0)));

		assertArrayEquals(new long[] {100, 98, 96, 94, 92},
			values(client.query(policy, topK(BIN, BinDataType.INTEGER, Order.DESC, 5)), BIN));
	}

	@Test
	public void indexFilterOnAnotherBinLimitsCandidates() {
		requireCapableCluster();

		Statement statement = topK(BIN, BinDataType.INTEGER, Order.DESC, 3);
		statement.setFilter(Filter.range(GROUP, 2, 3));

		assertArrayEquals(new long[] {40, 39, 38}, values(statement, BIN));
	}

	@Test
	public void bytesKeyComparesUnsigned() {
		requireCapableCluster();

		// k reaches past 0x80 (rank 28), so the client merges candidates from both sides of it.
		assertArrayEquals(ascending(1, 40), values(topK(BLOB, BinDataType.BYTES, Order.ASC, 40), BIN));
		assertArrayEquals(descending(COUNT, 80), values(topK(BLOB, BinDataType.BYTES, Order.DESC, 80), BIN));
	}

	@Test
	public void bytesSecondKeyBreaksTies() {
		requireCapableCluster();

		Statement statement = topK(GROUP, BinDataType.INTEGER, Order.DESC, 3);
		statement.thenOrderBy(BLOB, BinDataType.BYTES, Order.DESC);

		assertArrayEquals(new long[] {100, 99, 98}, values(statement, BIN));
	}

	@Test
	public void kAboveMatchCountReturnsEveryMatch() {
		requireCapableCluster();

		assertArrayEquals(descending(COUNT, COUNT), values(topK(BIN, BinDataType.INTEGER, Order.DESC, 1000), BIN));
	}

	@Test
	public void emptySetReturnsNothing() {
		requireCapableCluster();

		Statement statement = topK(BIN, BinDataType.INTEGER, Order.DESC, 10);
		statement.setSetName(EMPTY_SET);

		assertEquals(0, values(statement, BIN).length);
	}

	@Test(timeout = 10000)
	public void recordQueueSmallerThanK() {
		requireCapableCluster();

		QueryPolicy policy = new QueryPolicy();
		policy.recordQueueSize = 2;

		assertArrayEquals(descending(COUNT, 50),
			values(client.query(policy, topK(BIN, BinDataType.INTEGER, Order.DESC, 50)), BIN));
	}

	@Test(timeout = 10000)
	public void closeBeforeDrainingDoesNotHang() {
		requireCapableCluster();

		QueryPolicy policy = new QueryPolicy();
		policy.recordQueueSize = 2;

		RecordSet recordSet = client.query(policy, topK(BIN, BinDataType.INTEGER, Order.DESC, 50));

		try {
			assertTrue(recordSet.next());
			assertEquals(COUNT, recordSet.getRecord().getLong(BIN));
		}
		finally {
			recordSet.close();
		}

		assertArrayEquals(descending(COUNT, 3),
			values(client.query(policy, topK(BIN, BinDataType.INTEGER, Order.DESC, 3)), BIN));
	}

	@Test
	public void totalTimeoutSurfacesWithoutPartialRows() {
		requireCapableCluster();

		QueryPolicy policy = new QueryPolicy();
		policy.totalTimeout = 300;
		policy.maxRetries = 0;

		// Throttled so each node's scan outlasts the timeout; a node sends nothing until it finishes.
		Statement statement = topK(BIN, BinDataType.INTEGER, Order.DESC, 10);
		statement.setRecordsPerSecond(10);

		RecordSet recordSet = client.query(policy, statement);
		int rows = 0;
		AerospikeException error = null;

		try {
			while (recordSet.next()) {
				rows++;
			}
		}
		catch (AerospikeException ae) {
			error = ae;
		}
		finally {
			recordSet.close();
		}

		assertNotNull("Expected the total timeout to surface", error);
		assertEquals(ResultCode.TIMEOUT, error.getResultCode());
		assertEquals(0, rows);
	}

	@Test
	public void partitionRangeRanksOnlyThatRange() {
		requireCapableCluster();

		int begin = 0;
		int count = Node.PARTITIONS / 2;
		List<Long> inRange = new ArrayList<>();

		for (int i = 1; i <= COUNT; i++) {
			int partitionId = Partition.getPartitionId(key(i).digest);

			if (partitionId >= begin && partitionId < begin + count) {
				inRange.add((long)i);
			}
		}
		inRange.sort(Collections.reverseOrder());

		int k = 5;
		long[] expected = new long[k];

		for (int i = 0; i < k; i++) {
			expected[i] = inRange.get(i);
		}

		RecordSet recordSet = client.queryPartitions(null,
			topK(BIN, BinDataType.INTEGER, Order.DESC, k), PartitionFilter.range(begin, count));
		assertArrayEquals(expected, values(recordSet, BIN));
	}

	@Test
	public void queryNodeRanksThatNodesRecords() {
		requireCapableCluster();

		int k = 5;

		for (Node node : client.getNodes()) {
			long[] all = values(client.queryNode(null, base(BIN), node), BIN);
			Arrays.sort(all);

			long[] expected = new long[k];

			for (int i = 0; i < k; i++) {
				expected[i] = all[all.length - 1 - i];
			}

			long[] ranked = values(client.queryNode(null, topK(BIN, BinDataType.INTEGER, Order.DESC, k), node), BIN);
			assertArrayEquals(node.getName(), expected, ranked);
		}
	}

	private static Key key(int rank) {
		return new Key(args.namespace, SET, "key" + rank);
	}

	private Statement ranked(Order order, int k) {
		Statement statement = base(BIN);
		statement.setOrderBy(BIN, BinDataType.INTEGER, order);
		statement.setTopK(k);
		return statement;
	}

	private Statement rankedDerived(Order order, int k) {
		Statement statement = derived();
		statement.setOrderBy(DERIVED, BinDataType.INTEGER, order);
		statement.setTopK(k);
		return statement;
	}

	private Statement orderKeyOnly() {
		Statement statement = base(BIN);
		statement.setOrderBy(BIN, BinDataType.INTEGER, Order.DESC);
		return statement;
	}

	private Statement topK(String bin, BinDataType type, Order order, int k) {
		Statement statement = statement();
		statement.setOrderBy(bin, type, order);
		statement.setTopK(k);
		return statement;
	}

	private Statement base(String bin) {
		Statement statement = statement();
		statement.setBinNames(bin);
		return statement;
	}

	private Statement statement() {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		return statement;
	}

	private Statement derived() {
		Statement statement = statement();
		statement.setOperations(new Operation[] {
			ExpOperation.read(DERIVED,
				Exp.build(Exp.mul(Exp.intBin(BIN), Exp.val(10))),
				ExpReadFlags.DEFAULT)
		});
		return statement;
	}

	private static void drain(RecordSet recordSet) {
		try {
			while (recordSet.next()) {
			}
		}
		finally {
			recordSet.close();
		}
	}

	private static long[] ascending(int first, int count) {
		long[] values = new long[count];

		for (int i = 0; i < count; i++) {
			values[i] = first + i;
		}
		return values;
	}

	private static long[] descending(int first, int count) {
		long[] values = new long[count];

		for (int i = 0; i < count; i++) {
			values[i] = first - i;
		}
		return values;
	}

	private long[] values(Statement statement, String valueBin) {
		return values(client.query(null, statement), valueBin);
	}

	private long[] values(RecordSet recordSet, String valueBin) {
		List<Long> values = new ArrayList<>();

		try {
			while (recordSet.next()) {
				values.add(recordSet.getRecord().getLong(valueBin));
			}
		}
		finally {
			recordSet.close();
		}

		long[] ordered = new long[values.size()];

		for (int i = 0; i < ordered.length; i++) {
			ordered[i] = values.get(i);
		}
		return ordered;
	}

	private List<Object> rankedValues(Statement statement, String valueBin) {
		List<Object> values = new ArrayList<>();
		RecordSet recordSet = client.query(null, statement);

		try {
			while (recordSet.next()) {
				values.add(recordSet.getRecord().getValue(valueBin));
			}
		}
		finally {
			recordSet.close();
		}
		return values;
	}
}
