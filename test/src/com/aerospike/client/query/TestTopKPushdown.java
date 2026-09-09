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

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.test.sync.TestSync;

/**
 * Verifies Top-K results against a live cluster in both modes: client-only reduction over the
 * full query stream ({@link Statement#setReduce}) and server pushdown plus client merge
 * ({@link Statement#setOrderBy} + {@link Statement#setTopK}). Both must produce the same global
 * Top-K. The wire encoding of the pushdown request is covered separately by
 * {@code TestTopKRequestEncoding}.
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

	@Test
	public void clientOnlyReturnsGlobalTopK() {
		requireCapableCluster();

		assertArrayEquals(new long[] {100, 99, 98}, values(clientOnly(Order.DESC, 3), BIN));
		assertArrayEquals(new long[] {1, 2, 3}, values(clientOnly(Order.ASC, 3), BIN));
	}

	@Test
	public void pushdownReturnsGlobalTopK() {
		requireCapableCluster();

		assertArrayEquals(new long[] {100, 99, 98}, values(pushdown(Order.DESC, 3), BIN));
		assertArrayEquals(new long[] {1, 2, 3}, values(pushdown(Order.ASC, 3), BIN));
	}

	@Test
	public void bothModesAreEquivalent() {
		requireCapableCluster();

		for (Order order : new Order[] {Order.ASC, Order.DESC}) {
			for (int k : new int[] {1, 5, 25}) {
				long[] clientOnly = values(clientOnly(order, k), BIN);
				long[] pushdown = values(pushdown(order, k), BIN);

				assertEquals(k, pushdown.length);
				assertArrayEquals("Mismatch for order=" + order + " k=" + k, clientOnly, pushdown);
			}
		}
	}

	@Test
	public void expressionProjectedKeyIsEquivalent() {
		requireCapableCluster();

		int k = 5;
		long[] expected = new long[k];

		for (int i = 0; i < k; i++) {
			expected[i] = (i + 1) * 10L;
		}

		assertArrayEquals(expected, values(clientOnlyDerived(Order.ASC, k), DERIVED));
		assertArrayEquals(expected, values(pushdownDerived(Order.ASC, k), DERIVED));
	}

	private Statement clientOnly(Order order, int k) {
		Statement statement = base(BIN);
		statement.setReduce(Reduce.topK(BIN, BinDataType.INTEGER, order, OrderByFlags.NONE, k));
		return statement;
	}

	private Statement pushdown(Order order, int k) {
		Statement statement = base(BIN);
		statement.setOrderBy(BIN, BinDataType.INTEGER, order);
		statement.setTopK(k);
		return statement;
	}

	private Statement clientOnlyDerived(Order order, int k) {
		Statement statement = derived();
		statement.setReduce(Reduce.topK(DERIVED, BinDataType.INTEGER, order, OrderByFlags.NONE, k));
		return statement;
	}

	private Statement pushdownDerived(Order order, int k) {
		Statement statement = derived();
		statement.setOrderBy(DERIVED, BinDataType.INTEGER, order);
		statement.setTopK(k);
		return statement;
	}

	private Statement base(String bin) {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		statement.setBinNames(bin);
		return statement;
	}

	private Statement derived() {
		Statement statement = new Statement();
		statement.setNamespace(args.namespace);
		statement.setSetName(SET);
		statement.setOperations(new Operation[] {
			ExpOperation.read(DERIVED,
				Exp.build(Exp.mul(Exp.intBin(BIN), Exp.val(10))),
				ExpReadFlags.DEFAULT)
		});
		return statement;
	}

	private long[] values(Statement statement, String valueBin) {
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

		long[] ordered = new long[values.size()];

		for (int i = 0; i < ordered.length; i++) {
			ordered[i] = values.get(i);
		}
		return ordered;
	}
}
