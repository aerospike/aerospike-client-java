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
package com.aerospike.examples;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.BinDataType;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Order;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

/**
 * Demonstrate client-side Top-K reduce using {@link Statement#setOrderBy} + {@link Statement#setTopK}.
 * <p>
 * Note: the server does not yet send a per-node bounded Top-K result set for this reduce
 * (see docs/REDUCE-SPEC-DESIGN.md); every matching record is still sent to the client, which
 * merges them locally. Once {@link Statement#setTopK} is set, {@link IAerospikeClient#query}
 * applies the reduce internally and streams only the final merged Top-K records through the
 * returned {@link RecordSet} — no manual combiner handling is needed.
 */
public class QueryTopK extends Example {

	/**
	 * Query records and select the top K by bin value using {@code setOrderBy}/{@code setTopK}.
	 */
	@Override
	public void runExample() throws Exception {
		String indexName = "topkindex";
		String keyPrefix = "topkkey";
		String binName = "topkbin";
		int size = 20;
		int k = 5;

		createIndex(indexName, binName);
		writeRecords(keyPrefix, binName, size);
		runQuery(indexName, binName, k);
		client().dropIndex(readPolicy(), namespace(), set(), indexName);
	}

	private void createIndex(String indexName, String binName) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			namespace(), set(), indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client().createIndex(policy, namespace(), set(), indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void writeRecords(String keyPrefix, String binName, int size) throws Exception {
		console.info("Write " + size + " records.");

		// Values 1..size. The top 5 descending are size, size-1, size-2, size-3, size-4.
		for (int i = 1; i <= size; i++) {
			Key key = new Key(namespace(), set(), keyPrefix + i);
			Bin bin = new Bin(binName, i);
			client().put(writePolicy(), key, bin);
		}
	}

	private void runQuery(String indexName, String binName, int k) throws Exception {
		int begin = 1;
		int end = 1000;

		console.info("Query top %d by bin=%s descending", k, binName);

		Statement stmt = new Statement();
		stmt.setNamespace(namespace());
		stmt.setSetName(set());
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));

		stmt.setOrderBy(binName, BinDataType.INTEGER, Order.DESC);
		stmt.setTopK(k);

		// client().query() merges every node's results and streams back only the final,
		// globally-ordered top k records.
		try (RecordSet rs = client().query(null, stmt)) {
			int count = 0;
			int expected = 20; // largest value written

			while (rs.next()) {
				Record record = rs.getRecord();
				int value = record.getInt(binName);

				console.info("Top-K record: %s=%d", binName, value);

				if (value != expected) {
					console.error("Top-K order mismatch. Expected %d. Received %d.", expected, value);
				}
				expected--;
				count++;
			}

			if (count != k) {
				console.error("Top-K count mismatch. Expected %d. Received %d.", k, count);
			}
		}
	}
}
