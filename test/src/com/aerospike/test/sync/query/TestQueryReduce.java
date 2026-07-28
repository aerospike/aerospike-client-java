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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Reduce;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

/**
 * End-to-end tests for client-side Top-K map-reduce ({@link Reduce#topK}) executed across the
 * cluster.
 * <p>
 * All tests are {@link Ignore}d until a server build that supports the reduce feature is available.
 * They are kept in {@code SuiteSync} so they compile in CI but are skipped at runtime.
 */
@Ignore("Requires server build with reduce support")
public class TestQueryReduce extends TestSync {
	private static final String indexName = "reduceindex";
	private static final String keyPrefix = "reducekey";
	private static final String binName = "reducebin";
	private static final int size = 10;

	@BeforeClass
	public static void prepare() {
		try {
			IndexTask itask = client.createIndex(args.indexPolicy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
			itask.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			client.put(null, key, new Bin(binName, i));
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	private Statement baseStatement() {
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, 1, size));
		return stmt;
	}

	@Test
	public void topKDescending() {
		Statement stmt = baseStatement();
		int k = 3;
		stmt.setOrderBy(binName, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(k);

		assertTopK(stmt, k, new long[] {10, 9, 8});
	}

	@Test
	public void topKAscending() {
		Statement stmt = baseStatement();
		int k = 3;
		stmt.setOrderBy(binName, BinDataType.INTEGER, Order.ASC);
		stmt.setTopK(k);

		assertTopK(stmt, k, new long[] {1, 2, 3});
	}

	private void assertTopK(Statement stmt, int k, long[] expected) {
		RecordSet rs = client.query(null, stmt);
		int count = 0;

		try {
			while (rs.next()) {
				Record record = rs.getRecord();
				assertEquals(expected[count], record.getLong(binName));
				count++;
			}
		}
		finally {
			rs.close();
		}

		assertEquals(k, count);
	}
}
