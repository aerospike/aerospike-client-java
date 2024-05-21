/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.test.sync.TestSync;

public class TestQueryBlob extends TestSync {
	private static final String indexName = "qbindex";
	private static final String binName = "bb";
	private static final String indexNameList = "qblist";
	private static final String binNameList = "bblist";
	private static int size = 5;

	@BeforeClass
	public static void prepare() {
		IndexTask task = client.createIndex(args.indexPolicy, args.namespace, args.set, indexName, binName, IndexType.BLOB);
		task.waitTillComplete();

		task = client.createIndex(args.indexPolicy, args.namespace, args.set, indexNameList, binNameList, IndexType.BLOB, IndexCollectionType.LIST);
		task.waitTillComplete();

		for (int i = 1; i <= size; i++) {
			byte[] bytes = new byte[8];
			Buffer.longToBytes(50000 + i, bytes, 0);

			List<byte[]> list = new ArrayList<>();
			list.add(bytes);

			Key key = new Key(args.namespace, args.set, i);
			Bin bin = new Bin(binName, bytes);
			Bin binList = new Bin(binNameList, list);
			client.put(null, key, bin, binList);
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
		client.dropIndex(null, args.namespace, args.set, indexNameList);
	}

	@Test
	public void queryBlob() {
		byte[] bytes = new byte[8];
		Buffer.longToBytes(50003, bytes, 0);

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.equal(binName, bytes));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();
				byte[] result = record.getBytes(binName);
				assertTrue(Arrays.equals(bytes, result));
				count++;
			}

			assertNotEquals(0, count);
		}
		finally {
			rs.close();
		}
	}

	@Test
	public void queryBlobInList() {
		byte[] bytes = new byte[8];
		Buffer.longToBytes(50003, bytes, 0);

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName, binNameList);
		stmt.setFilter(Filter.contains(binNameList, IndexCollectionType.LIST, bytes));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				Record record = rs.getRecord();

				List<?> list = record.getList(binNameList);
				assertEquals(1, list.size());

				byte[] result = (byte[])list.get(0);
				assertTrue(Arrays.equals(bytes, result));
				count++;
			}

			assertEquals(1, count);
		}
		finally {
			rs.close();
		}
	}
}
