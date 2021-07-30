/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

public class TestBatch extends TestSync {
	private static final String BinName = "batchbin";
	private static final String ListBin = "listbin";
	private static final String KeyPrefix = "batchkey";
	private static final String ValuePrefix = "batchvalue";
	private static final int Size = 8;

	@BeforeClass
	public static void writeRecords() {
		WritePolicy policy = new WritePolicy();
		policy.expiration = 2592000;

		for (int i = 1; i <= Size; i++) {
			Key key = new Key(args.namespace, args.set, KeyPrefix + i);
			Bin bin = new Bin(BinName, ValuePrefix + i);

			List<Integer> list = new ArrayList<Integer>();

			for (int j = 0; j < i; j++) {
				list.add(j * i);
			}

			Bin listBin = new Bin(ListBin, list);

			if (i != 6) {
				client.put(policy, key, bin, listBin);
			}
			else {
				client.put(policy, key, new Bin(BinName, i), listBin);
			}
		}
	}

	@Test
	public void batchExists () {
		Key[] keys = new Key[Size];
		for (int i = 0; i < Size; i++) {
			keys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		boolean[] existsArray = client.exists(null, keys);
		assertEquals(Size, existsArray.length);

		for (int i = 0; i < existsArray.length; i++) {
			if (! existsArray[i]) {
				fail("Some batch records not found.");
			}
		}
	}

	@Test
	public void batchReads () {
		Key[] keys = new Key[Size];
		for (int i = 0; i < Size; i++) {
			keys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		Record[] records = client.get(null, keys, BinName);
		assertEquals(Size, records.length);

		for (int i = 0; i < records.length; i++) {
			Key key = keys[i];
			Record record = records[i];

			if (i != 5) {
				assertBinEqual(key, record, BinName, ValuePrefix + (i + 1));
			}
			else {
				assertBinEqual(key, record, BinName, i + 1);
			}
		}
	}

	@Test
	public void batchReadHeaders () {
		Key[] keys = new Key[Size];
		for (int i = 0; i < Size; i++) {
			keys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		Record[] records = client.getHeader(null, keys);
		assertEquals(Size, records.length);

		for (int i = 0; i < records.length; i++) {
			Key key = keys[i];
			Record record = records[i];

			assertRecordFound(key, record);
			assertNotEquals(0, record.generation);
			assertNotEquals(0, record.expiration);
		}
	}

	@Test
	public void batchReadComplex () {
		// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.

		// bin * 8
		Expression exp = Exp.build(Exp.mul(Exp.intBin(BinName), Exp.val(8)));
		Operation[] ops = Operation.array(ExpOperation.read(BinName, exp, ExpReadFlags.DEFAULT));

		String[] bins = new String[] {BinName};
		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 1), bins));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 2), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 3), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 4), false));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 5), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 6), ops));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 7), bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 8), new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, "keynotfound"), bins));

		// Execute batch.
		client.get(null, records);

		assertBatchBinEqual(records, BinName, 0);
		assertBatchBinEqual(records, BinName, 1);
		assertBatchBinEqual(records, BinName, 2);
		assertBatchRecordExists(records, BinName, 3);
		assertBatchBinEqual(records, BinName, 4);

		BatchRead batch = records.get(5);
		assertRecordFound(batch.key, batch.record);
		int v = batch.record.getInt(BinName);
		assertEquals(48, v);

		assertBatchBinEqual(records, BinName, 6);

		batch = records.get(7);
		assertRecordFound(batch.key, batch.record);
		Object val = batch.record.getValue("binnotfound");
		if (val != null) {
			fail("Unexpected batch bin value received");
		}

		batch = records.get(8);
		if (batch.record != null) {
			fail("Unexpected batch record received");
		}
	}

	@Test
	public void batchListOperate() {
		Key[] keys = new Key[Size];
		for (int i = 0; i < Size; i++) {
			keys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		// Get size and last element of list bin for all records.
		Record[] records = client.get(null, keys,
			ListOperation.size(ListBin),
			ListOperation.getByIndex(ListBin, -1, ListReturnType.VALUE)
			);

		for (int i = 0; i < records.length; i++) {
			Record record = records[i];
			List<?> results = record.getList(ListBin);
			long size = (Long)results.get(0);
			long val = (Long)results.get(1);

			assertEquals(i + 1, size);
			assertEquals(i * (i + 1), val);
		}
	}

	private void assertBatchBinEqual(List<BatchRead> list, String binName, int i) {
		BatchRead batch = list.get(i);
		assertBinEqual(batch.key, batch.record, binName, ValuePrefix + (i + 1));
	}

	private void assertBatchRecordExists(List<BatchRead> list, String binName, int i) {
		BatchRead batch = list.get(i);
		assertRecordFound(batch.key, batch.record);
		assertNotEquals(0, batch.record.generation);
		assertNotEquals(0, batch.record.expiration);
	}
}
