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
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

public class TestBatch extends TestSync {
	private static final String keyPrefix = "batchkey";
	private static final String valuePrefix = "batchvalue";
	private static final String binName = args.getBinName("batchbin");
	private static final int size = 8;

	@BeforeClass
	public static void writeRecords() {
		WritePolicy policy = new WritePolicy();
		policy.expiration = 2592000;

		for (int i = 1; i <= size; i++) {
			Key key = new Key(args.namespace, args.set, keyPrefix + i);
			Bin bin = new Bin(binName, valuePrefix + i);

			client.put(policy, key, bin);
		}
	}

	@Test
	public void batchExists () {
		Key[] keys = new Key[size];
		for (int i = 0; i < size; i++) {
			keys[i] = new Key(args.namespace, args.set, keyPrefix + (i + 1));
		}

		boolean[] existsArray = client.exists(null, keys);
		assertEquals(size, existsArray.length);

		for (int i = 0; i < existsArray.length; i++) {
			if (! existsArray[i]) {
				fail("Some batch records not found.");
			}
        }
    }

	@Test
	public void batchReads () {
		Key[] keys = new Key[size];
		for (int i = 0; i < size; i++) {
			keys[i] = new Key(args.namespace, args.set, keyPrefix + (i + 1));
		}

		Record[] records = client.get(null, keys, binName);
		assertEquals(size, records.length);

		for (int i = 0; i < records.length; i++) {
			Key key = keys[i];
			Record record = records[i];

			assertBinEqual(key, record, binName, valuePrefix + (i + 1));
        }
    }

	@Test
	public void batchReadHeaders () {
		Key[] keys = new Key[size];
		for (int i = 0; i < size; i++) {
			keys[i] = new Key(args.namespace, args.set, keyPrefix + (i + 1));
		}

		Record[] records = client.getHeader(null, keys);
		assertEquals(size, records.length);

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
		String[] bins = new String[] {binName};
		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 1), bins));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 2), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 3), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 4), false));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 5), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 6), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 7), bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, keyPrefix + 8), new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(new Key(args.namespace, args.set, "keynotfound"), bins));

		// Execute batch.
		client.get(null, records);

		assertBatchBinEqual(records, binName, 0);
		assertBatchBinEqual(records, binName, 1);
		assertBatchBinEqual(records, binName, 2);
		assertBatchRecordExists(records, binName, 3);
		assertBatchBinEqual(records, binName, 4);
		assertBatchBinEqual(records, binName, 5);
		assertBatchBinEqual(records, binName, 6);

		BatchRead batch = records.get(7);
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

	private void assertBatchBinEqual(List<BatchRead> list, String binName, int i) {
		BatchRead batch = list.get(i);
		assertBinEqual(batch.key, batch.record, binName, valuePrefix + (i + 1));
	}

	private void assertBatchRecordExists(List<BatchRead> list, String binName, int i) {
		BatchRead batch = list.get(i);
		assertRecordFound(batch.key, batch.record);
		assertNotEquals(0, batch.record.generation);
		assertNotEquals(0, batch.record.expiration);
	}
}
