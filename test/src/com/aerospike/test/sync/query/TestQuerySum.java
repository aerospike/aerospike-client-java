/*
 * Copyright 2012-2020 Aerospike, Inc.
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
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
import com.aerospike.test.sync.TestSync;

public class TestQuerySum extends TestSync {
	private static final String indexName = "aggindex";
	private static final String keyPrefix = "aggkey";
	private static final String binName = args.getBinName("aggbin");
	private static final int size = 10;

	@BeforeClass
	public static void prepare() {
		RegisterTask rtask = client.register(null, TestQuerySum.class.getClassLoader(), "udf/sum_example.lua", "sum_example.lua", Language.LUA);
		rtask.waitTillComplete();

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask itask = client.createIndex(policy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
			itask.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);
			client.put(null, key, bin);
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	@Test
	public void querySum() {
		int begin = 4;
		int end = 7;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));
		stmt.setAggregateFunction(TestQuerySum.class.getClassLoader(), "udf/sum_example.lua", "sum_example", "sum_single_bin", Value.get(binName));

		ResultSet rs = client.queryAggregate(null, stmt);

		try {
			int expected = 22; // 4 + 5 + 6 + 7
			int count = 0;

			while (rs.next()) {
				Object object = rs.getObject();
				long sum = 0;

				if (object instanceof Long) {
					sum = (Long)rs.getObject();
				}
				else {
					fail("Return value not a long: " + object);
				}
				assertEquals(expected, (int)sum);
				count++;
			}
			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}
}
