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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

/**
 * Top-K pushdown edge cases against a live cluster: node concurrency, filters, string and bytes
 * order keys, k bounds, the record queue, timeouts, partition ranges, and single-node queries.
 */
public class TestTopKEdgeCases extends TestSync {
	private static final String SET = "topKEdges";
	private static final String EMPTY_SET = "topKEdgesEmpty";
	private static final String INDEX = "topKEdgesGroup";
	private static final String RANK = "rank";
	private static final String GROUP = "group";
	private static final String NAME = "name";
	private static final String BLOB = "blob";
	private static final int COUNT = 100;

	@BeforeClass
	public static void prepare() {
		Assume.assumeTrue("Top-K pushdown requires server version 8.2.1 or later on every node",
			client.getCluster().hasQueryOrderBy);

		for (int i = 1; i <= COUNT; i++) {
			// Even ranks start upper case, so the case-sensitive and case-insensitive orders differ.
			String name = (i % 2 == 0 ? "Item" : "item") + String.format("%03d", i);
			// First bytes run from 0x65 to 0xc8, across 0x7f/0x80, where a signed comparison
			// inverts the order.
			byte[] blob = {(byte)(i + 100), 0};

			client.put(null, key(i), new Bin(RANK, i), new Bin(GROUP, (i - 1) / 10),
				new Bin(NAME, name), new Bin(BLOB, blob));
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
		if (! client.getCluster().hasQueryOrderBy) {
			return;
		}

		client.dropIndex(null, args.namespace, SET, INDEX);

		for (int i = 1; i <= COUNT; i++) {
			client.delete(null, key(i));
		}
	}

	@Test
	public void everyNodeConcurrencyReturnsGlobalTopK() {
		// The serial (1) and thread-chaining (2) paths need a cluster of at least three nodes.
		for (int maxConcurrentNodes : new int[] {0, 1, 2}) {
			QueryPolicy policy = new QueryPolicy();
			policy.maxConcurrentNodes = maxConcurrentNodes;

			assertArrayEquals("maxConcurrentNodes=" + maxConcurrentNodes, descending(COUNT, 10),
				ranks(client.query(policy, topK(RANK, BinDataType.INTEGER, Order.DESC, 10))));
		}
	}

	@Test
	public void filterExpressionLimitsCandidates() {
		QueryPolicy policy = new QueryPolicy();
		policy.filterExp = Exp.build(Exp.eq(Exp.mod(Exp.intBin(RANK), Exp.val(2)), Exp.val(0)));

		assertArrayEquals(new long[] {100, 98, 96, 94, 92},
			ranks(client.query(policy, topK(RANK, BinDataType.INTEGER, Order.DESC, 5))));
	}

	@Test
	public void indexFilterOnAnotherBinLimitsCandidates() {
		Statement statement = topK(RANK, BinDataType.INTEGER, Order.DESC, 3);
		statement.setFilter(Filter.range(GROUP, 2, 3));

		assertArrayEquals(new long[] {40, 39, 38}, ranks(client.query(null, statement)));
	}

	@Test
	public void stringKeyIsCaseSensitiveUnlessFlagged() {
		// The server compares UTF-8 bytes, so upper case ranks before lower case.
		assertArrayEquals(new long[] {2, 4, 6},
			ranks(client.query(null, topK(NAME, BinDataType.STRING, Order.ASC, 3))));

		Statement statement = topK(NAME, BinDataType.STRING, Order.ASC, 3);
		statement.setOrderBy(NAME, BinDataType.STRING, Order.ASC, OrderByFlags.CASE_INSENSITIVE);

		assertArrayEquals(new long[] {1, 2, 3}, ranks(client.query(null, statement)));
	}

	@Test
	public void stringKeyOrdersByUtf8NotUtf16() {
		// UTF-16 puts U+10000 (a surrogate pair) before U+FFFF; UTF-8 puts it after.
		Key bmp = new Key(args.namespace, SET, "bmp");
		Key supplementary = new Key(args.namespace, SET, "supplementary");
		client.put(null, bmp, new Bin(RANK, COUNT + 1), new Bin(NAME, "\uFFFF"));
		client.put(null, supplementary, new Bin(RANK, COUNT + 2), new Bin(NAME, "\uD800\uDC00"));

		try {
			long[] ranked = ranks(client.query(null, topK(NAME, BinDataType.STRING, Order.DESC, 2)));
			assertArrayEquals(new long[] {COUNT + 2, COUNT + 1}, ranked);
		}
		finally {
			client.delete(null, bmp);
			client.delete(null, supplementary);
		}
	}

	@Test
	public void bytesKeyComparesUnsigned() {
		// k reaches past 0x80 (rank 28), so the client merges candidates from both sides of it.
		assertArrayEquals(ascending(1, 40),
			ranks(client.query(null, topK(BLOB, BinDataType.BYTES, Order.ASC, 40))));
		assertArrayEquals(descending(COUNT, 80),
			ranks(client.query(null, topK(BLOB, BinDataType.BYTES, Order.DESC, 80))));
	}

	@Test
	public void stringSecondKeyBreaksTies() {
		Statement statement = topK(GROUP, BinDataType.INTEGER, Order.DESC, 3);
		statement.thenOrderBy(NAME, BinDataType.STRING, Order.ASC);

		assertArrayEquals(new long[] {92, 94, 96}, ranks(client.query(null, statement)));
	}

	@Test
	public void kAboveMatchCountReturnsEveryMatch() {
		assertArrayEquals(descending(COUNT, COUNT),
			ranks(client.query(null, topK(RANK, BinDataType.INTEGER, Order.DESC, 1000))));
	}

	@Test
	public void emptySetReturnsNothing() {
		Statement statement = topK(RANK, BinDataType.INTEGER, Order.DESC, 10);
		statement.setSetName(EMPTY_SET);

		assertEquals(0, ranks(client.query(null, statement)).length);
	}

	@Test(timeout = 10000)
	public void recordQueueSmallerThanK() {
		QueryPolicy policy = new QueryPolicy();
		policy.recordQueueSize = 2;

		assertArrayEquals(descending(COUNT, 50),
			ranks(client.query(policy, topK(RANK, BinDataType.INTEGER, Order.DESC, 50))));
	}

	@Test(timeout = 10000)
	public void closeBeforeDrainingDoesNotHang() {
		QueryPolicy policy = new QueryPolicy();
		policy.recordQueueSize = 2;

		RecordSet recordSet = client.query(policy, topK(RANK, BinDataType.INTEGER, Order.DESC, 50));

		try {
			assertTrue(recordSet.next());
			assertEquals(COUNT, recordSet.getRecord().getLong(RANK));
		}
		finally {
			recordSet.close();
		}

		assertArrayEquals(descending(COUNT, 3),
			ranks(client.query(policy, topK(RANK, BinDataType.INTEGER, Order.DESC, 3))));
	}

	@Test
	public void totalTimeoutSurfacesWithoutPartialRows() {
		QueryPolicy policy = new QueryPolicy();
		policy.totalTimeout = 300;
		policy.maxRetries = 0;

		// Throttled so each node's scan outlasts the timeout; a node sends nothing until it finishes.
		Statement statement = topK(RANK, BinDataType.INTEGER, Order.DESC, 10);
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
			topK(RANK, BinDataType.INTEGER, Order.DESC, k), PartitionFilter.range(begin, count));
		assertArrayEquals(expected, ranks(recordSet));
	}

	@Test
	public void queryNodeRanksThatNodesRecords() {
		int k = 5;

		for (Node node : client.getNodes()) {
			Statement clientOnly = base();
			clientOnly.setReduce(Reduce.topK(RANK, BinDataType.INTEGER, Order.DESC, OrderByFlags.NONE, k));

			long[] expected = ranks(client.queryNode(null, clientOnly, node));
			long[] pushdown = ranks(client.queryNode(null, topK(RANK, BinDataType.INTEGER, Order.DESC, k), node));

			assertEquals(node.getName(), k, pushdown.length);
			assertArrayEquals(node.getName(), expected, pushdown);
		}
	}

	private static Key key(int rank) {
		return new Key(args.namespace, SET, "key" + rank);
	}

	private static Statement base() {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		return statement;
	}

	private static Statement topK(String bin, BinDataType type, Order order, int k) {
		Statement statement = base();
		statement.setOrderBy(bin, type, order);
		statement.setTopK(k);
		return statement;
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

	private static long[] ranks(RecordSet recordSet) {
		List<Long> values = new ArrayList<>();

		try {
			while (recordSet.next()) {
				values.add(recordSet.getRecord().getLong(RANK));
			}
		}
		finally {
			recordSet.close();
		}

		long[] ranked = new long[values.size()];

		for (int i = 0; i < ranked.length; i++) {
			ranked[i] = values.get(i);
		}
		return ranked;
	}
}
