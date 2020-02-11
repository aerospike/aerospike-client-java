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
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryExecute extends TestSync {
	private static final String indexName = "qeindex1";
	private static final String keyPrefix = "qekey";
	private static final String binName1 = args.getBinName("qebin1");
	private static final String binName2 = args.getBinName("qebin2");
	private static final int size = 10;

	@BeforeClass
	public static void prepare() {
		RegisterTask rtask = client.register(null, TestQueryExecute.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		rtask.waitTillComplete();

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask itask = client.createIndex(policy, args.namespace, args.set, indexName, binName1, IndexType.NUMERIC);
			itask.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			client.put(null, key, new Bin(binName1, i), new Bin(binName2, i));
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	@Test
	public void queryExecute() {
		int begin = 3;
		int end = 9;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		ExecuteTask task = client.execute(null, stmt, "record_example", "processRecord", Value.get(binName1), Value.get(binName2), Value.get(100));
		task.waitTillComplete(3000, 3000);
		validateRecords();
	}

	private void validateRecords() {
		int begin = 1;
		int end = size + 100;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		RecordSet rs = client.query(null, stmt);

		try {
			int[] expectedList = new int[] {1,2,3,104,5,106,7,108,-1,10};
			int expectedSize = size - 1;
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				int value1 = record.getInt(binName1);
				int value2 = record.getInt(binName2);

				int val1 = value1;

				if (val1 == 9) {
					fail("Data mismatch. value1 " + val1 + " should not exist");
				}

				if (val1 == 5) {
					if (value2 != 0) {
						fail("Data mismatch. value2 " + value2 + " should be null");
					}
				}
				else if (value1 != expectedList[value2-1]) {
					fail("Data mismatch. Expected " + expectedList[value2-1] + ". Received " + value1);
				}
				count++;
			}
			assertEquals(expectedSize, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryExecuteOperate() {
		int begin = 3;
		int end = 9;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		Bin bin = new Bin("foo", "bar");

		ExecuteTask task = client.execute(null, stmt, Operation.put(bin));
		task.waitTillComplete(3000, 3000);

		String expected = bin.value.toString();

		stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setFilter(Filter.range(binName1, begin, end));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				String value = record.getString(bin.name);

				if (value == null) {
					fail("Bin " + bin.name + " not found");
				}

				if (! value.equals(expected)) {
					fail("Data mismatch. Expected " + expected + ". Received " + value);
				}
				count++;
			}
			assertEquals(end - begin + 1, count);
		}
		finally {
			rs.close();
		}
	}
}
