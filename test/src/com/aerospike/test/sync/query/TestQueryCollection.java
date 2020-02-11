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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryCollection extends TestSync {
	private static final String indexName = "mapkey_index";
	private static final String keyPrefix = "qkey";
	private static final String mapKeyPrefix = "mkey";
	private static final String mapValuePrefix = "qvalue";
	private static final String binName = args.getBinName("map_bin");
	private static final int size = 20;

	@BeforeClass
	public static void prepare() {
		RegisterTask rtask = client.register(null, TestQueryCollection.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		rtask.waitTillComplete();

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, args.namespace, args.set, indexName, binName, IndexType.STRING, IndexCollectionType.MAPKEYS);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			HashMap<String,String> map = new HashMap<String,String>();

			map.put(mapKeyPrefix+1, mapValuePrefix+i);
			if (i%2 == 0) {
				map.put(mapKeyPrefix+2, mapValuePrefix+i);
			}
			if (i%3 == 0 ) {
				map.put(mapKeyPrefix+3, mapValuePrefix+i);
			}

			Bin bin = new Bin(binName, map);
			client.put(null, key, bin);
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	@Test
	public void queryCollection() {
		String queryMapKey = mapKeyPrefix+2;
		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.contains(binName, IndexCollectionType.MAPKEYS, queryMapKey));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				Map<?,?> result = (Map<?,?>)record.getValue(binName);

				if (! result.containsKey(queryMapKey)) {
					fail("Query mismatch: Expected mapKey " + queryMapKey + " Received " + result);
				}
				count++;
			}
			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}
}
