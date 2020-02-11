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
import static org.junit.Assert.assertNotNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryKey extends TestSync {
	private static final String indexName = "skindex";
	private static final String keyPrefix = "skkey";
	private static final String binName = args.getBinName("skbin");
	private static final int size = 10;

	@BeforeClass
	public static void prepare() {
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

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.sendKey = true;

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);
			client.put(writePolicy, key, bin);
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	@Test
	public void queryKey() {
		int begin = 2;
		int end = 5;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Key key = rs.getKey();
				assertNotNull(key.userKey);

				Object userkey = key.userKey.getObject();
				assertNotNull(userkey);
				count++;
			}
			assertEquals(4, count);
		}
		finally {
			rs.close();
		}
	}
}
