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

import java.util.Map;

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

public class TestQueryFilter extends TestSync {
	private static final String indexName = "profileindex";
	private static final String keyPrefix = "profilekey";
	private static final String binName = args.getBinName("name");

	@BeforeClass
	public static void prepare() {
		RegisterTask rtask = client.register(null, TestQueryFilter.class.getClassLoader(), "udf/filter_example.lua", "filter_example.lua", Language.LUA);
		rtask.waitTillComplete();

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask itask = client.createIndex(policy, args.namespace, args.set, indexName, binName, IndexType.STRING);
			itask.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		writeRecord(keyPrefix + 1, "Charlie", "cpass");
		writeRecord(keyPrefix + 2, "Bill", "hknfpkj");
		writeRecord(keyPrefix + 3, "Doug", "dj6554");
	}

	private static void writeRecord(String userKey, String name, String password) {
		Key key = new Key(args.namespace, args.set, userKey);
		Bin bin1 = new Bin("name", name);
		Bin bin2 = new Bin("password", password);
		client.put(null, key, bin1, bin2);
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void queryFilter() {
		String nameFilter = "Bill";
		String passFilter = "hknfpkj";

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.equal(binName, nameFilter));
		stmt.setAggregateFunction(TestQueryFilter.class.getClassLoader(), "udf/filter_example.lua", "filter_example", "profile_filter", Value.get(passFilter));

		// passFilter will be applied in filter_example.lua.
		ResultSet rs = client.queryAggregate(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Map<String,Object> map = (Map<String,Object>)rs.getObject();
				assertEquals(nameFilter, map.get("name"));
				assertEquals(passFilter, map.get("password"));
				count++;
			}
			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}
}
