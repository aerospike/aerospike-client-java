/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.aerospike.client.query;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.test.sync.TestSync;

/**
 * Verifies client-only and pushed-down Top-K behavior against a live cluster.
 */
public class TestTopKPushdown extends TestSync {
	private static final String SET = "topKPushdown";
	private static final String BIN = "rank";
	private static final String DERIVED = "derived";
	private static final int COUNT = 100;

	@BeforeClass
	public static void prepare() {
		for (int i = 1; i <= COUNT; i++) {
			client.put(null, new Key(args.namespace, SET, "key" + i), new Bin(BIN, i));
		}
	}

	@AfterClass
	public static void destroy() {
		for (int i = 1; i <= COUNT; i++) {
			client.delete(null, new Key(args.namespace, SET, "key" + i));
		}
	}

	private static void requireCapableCluster() {
		Node[] nodes = client.getCluster().getNodes();
		Assume.assumeTrue(nodes.length > 0);

		for (Node node : nodes) {
			Assume.assumeTrue("Cluster must advertise query-order-by", node.hasQueryOrderBy());
		}
	}

	private static int nodeCount() {
		return client.getCluster().getNodes().length;
	}

	@Test
	public void clientOnlyReducesFullStream() {
		requireCapableCluster();

		Result result = run(Order.DESC, 3, false);

		assertArrayEquals(new long[] {100, 99, 98}, result.values);
		assertEquals("Client-only mode must receive every matching record", COUNT, result.inbound);
	}

	@Test
	public void serverBoundsCandidates() {
		requireCapableCluster();

		int k = 3;
		Result result = run(Order.DESC, k, true);

		assertArrayEquals(new long[] {100, 99, 98}, result.values);
		assertTrue("Server must bound each node to at most k", result.inbound <= (long)k * nodeCount());
		assertTrue("Server-side bounding must reduce inbound below full data set", result.inbound < COUNT);
	}

	@Test
	public void bothModesAreEquivalent() {
		requireCapableCluster();

		for (Order order : new Order[] {Order.ASC, Order.DESC}) {
			for (int k : new int[] {1, 5, 25}) {
				Result clientOnly = run(order, k, false);
				Result pushdown = run(order, k, true);

				assertArrayEquals(
					"Mismatch for order=" + order + " k=" + k, clientOnly.values, pushdown.values);
				assertEquals(k, pushdown.values.length);

				assertEquals(COUNT, clientOnly.inbound);
				assertTrue(pushdown.inbound <= (long)k * nodeCount());
			}
		}
	}

	@Test
	public void expressionProjectedKeyIsEquivalent() {
		requireCapableCluster();

		int k = 5;
		Result clientOnly = runDerived(Order.ASC, k, false);
		Result pushdown = runDerived(Order.ASC, k, true);

		long[] expected = new long[k];
		for (int i = 0; i < k; i++) {
			expected[i] = (i + 1) * 10L;
		}

		assertArrayEquals(expected, clientOnly.values);
		assertArrayEquals(clientOnly.values, pushdown.values);
		assertEquals(COUNT, clientOnly.inbound);
		assertTrue(pushdown.inbound <= (long)k * nodeCount());
	}

	private Result run(Order order, int k, boolean pushdown) {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		statement.setBinNames(BIN);
		statement.setOrderBy(BIN, BinDataType.INTEGER, order);
		statement.setTopK(k);
		statement.setTopKPushdownEnabled(pushdown);

		return execute(statement, BIN);
	}

	private Result runDerived(Order order, int k, boolean pushdown) {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		statement.setOperations(new Operation[] {
			ExpOperation.read(DERIVED,
				Exp.build(Exp.mul(Exp.intBin(BIN), Exp.val(10))),
				ExpReadFlags.DEFAULT)
		});
		statement.setOrderBy(DERIVED, BinDataType.INTEGER, order);
		statement.setTopK(k);
		statement.setTopKPushdownEnabled(pushdown);

		return execute(statement, DERIVED);
	}

	private Result execute(Statement statement, String valueBin) {
		List<Long> values = new ArrayList<>();
		RecordSet recordSet = client.query(null, statement);

		try {
			while (recordSet.next()) {
				values.add(recordSet.getRecord().getLong(valueBin));
			}
		}
		finally {
			recordSet.close();
		}

		ReduceSpec<Record, Record> spec = statement.resolveReduce();
		TopKReduceSpec reducer = (TopKReduceSpec)spec;
		long[] ordered = new long[values.size()];

		for (int i = 0; i < ordered.length; i++) {
			ordered[i] = values.get(i);
		}
		return new Result(ordered, reducer.getInputCount());
	}

	private static final class Result {
		final long[] values;
		final int inbound;

		Result(long[] values, int inbound) {
			this.values = values;
			this.inbound = inbound;
		}
	}
}
