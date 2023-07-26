/*
 * Copyright 2012-2023 Aerospike, Inc.
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
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class QuerySum extends Example {

	public QuerySum(Console console) {
		super(console);
	}

	/**
	 * Query records and calculate sum using a user-defined aggregation function.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		String indexName = "aggindex";
		String keyPrefix = "aggkey";
		String binName = "aggbin";
		int size = 10;

		register(client, params);
		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, binName, size);
		runQuery(client, params, indexName, binName);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);
	}

	private void register(IAerospikeClient client, Parameters params) throws Exception {
		RegisterTask task = client.register(params.policy, "udf/sum_example.lua", "sum_example.lua", Language.LUA);
		// Alternately register from resource.
		// RegisterTask task = client.register(params.policy, QuerySum.class.getClassLoader(), "udf/sum_example.lua", "sum_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	private void createIndex(
		IAerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			params.namespace, params.set, indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void writeRecords(
		IAerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		int size
	) throws Exception {
		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);

			console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, bin.value);

			client.put(params.writePolicy, key, bin);
		}
	}

	private void runQuery(
		IAerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {

		int begin = 4;
		int end = 7;

		console.info("Query for: ns=%s set=%s index=%s bin=%s >= %s <= %s",
			params.namespace, params.set, indexName, binName, begin, end);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setAggregateFunction("sum_example", "sum_single_bin", Value.get(binName));
		// Alternately load aggregate function from resource
		// stmt.setAggregateFunction(QuerySum.class.getClassLoader(), "udf/sum_example.lua", "sum_example", "sum_single_bin", Value.get(binName));

		ResultSet rs = client.queryAggregate(null, stmt);

		try {
			int expected = 22; // 4 + 5 + 6 + 7
			int count = 0;

			while (rs.next()) {
				Object object = rs.getObject();
				long sum;

				if (object instanceof Long) {
					sum = (Long)rs.getObject();
				}
				else {
					console.error("Return value not a long: " + object);
					continue;
				}

				if (expected == (int)sum) {
					console.info("Sum matched: value=%d", expected);
				}
				else {
					console.error("Sum mismatch: Expected %d. Received %d.", expected, (int)sum);
				}
				count++;
			}

			if (count == 0) {
				console.error("Query failed. No records returned.");
			}
		}
		finally {
			rs.close();
		}
	}
}
