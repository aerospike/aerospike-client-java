/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchReadPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import com.aerospike.test.sync.TestSync;

public class TestBatch extends TestSync {
	private static final String BinName = "bbin";
	private static final String BinName2 = "bbin2";
	private static final String BinName3 = "bbin3";
	private static final String ListBin = "lbin";
	private static final String ListBin2 = "lbin2";
	private static final String KeyPrefix = "batchkey";
	private static final String ValuePrefix = "batchvalue";
	private static final int Size = 8;

	@BeforeClass
	public static void writeRecords() {
		WritePolicy policy = new WritePolicy();

		if (args.hasTtl) {
			policy.expiration = 2592000;
		}

		for (int i = 1; i <= Size; i++) {
			Key key = new Key(args.namespace, args.set, KeyPrefix + i);
			Bin bin = new Bin(BinName, ValuePrefix + i);

			List<Integer> list = new ArrayList<Integer>();

			for (int j = 0; j < i; j++) {
				list.add(j * i);
			}

			List<Integer> list2 = new ArrayList<Integer>();

			for (int j = 0; j < 2; j++) {
				list2.add(j);
			}

			Bin listBin = new Bin(ListBin, list);
			Bin listBin2 = new Bin(ListBin2, list2);

			if (i != 6) {
				client.put(policy, key, bin, listBin, listBin2);
			}
			else {
				client.put(policy, key, new Bin(BinName, i), listBin, listBin2);
			}
		}

		// Add records that will eventually be deleted.
		client.put(policy, new Key(args.namespace, args.set, 10000), new Bin(BinName, 10000));
		client.put(policy, new Key(args.namespace, args.set, 10001), new Bin(BinName, 10001));
		client.put(policy, new Key(args.namespace, args.set, 10002), new Bin(BinName, 10002));
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
	public void batchReadsEmptyBinNames() {
		Key[] keys = new Key[Size];
		for (int i = 0; i < Size; i++) {
			keys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		String[] binNames = new String[] {};
		Record[] records = client.get(null, keys, binNames);
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

			if (args.hasTtl) {
				assertNotEquals(0, record.expiration);
			}
		}
	}

	@Test
	public void batchReadComplex() {
		// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.

		// bin * 8
		Expression exp = Exp.build(Exp.mul(Exp.intBin(BinName), Exp.val(8)));
		Operation[] ops = Operation.array(ExpOperation.read(BinName, exp, ExpReadFlags.DEFAULT));

		String[] bins = new String[] {BinName};
		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 1), bins));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 2), true));
		records.add(new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 3), new String[] {}));
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
	public void batchListReadOperate() {
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

	@Test
	public void batchListWriteOperate() {
		Key[] keys = new Key[Size];
		for (int i = 0; i < Size; i++) {
			keys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		// Add integer to list and get size and last element of list bin for all records.
		BatchResults bresults = client.operate(null, null, keys,
			ListOperation.insert(ListBin2, 0, Value.get(1000)),
			ListOperation.size(ListBin2),
			ListOperation.getByIndex(ListBin2, -1, ListReturnType.VALUE)
			);

		for (int i = 0; i < bresults.records.length; i++) {
			BatchRecord br = bresults.records[i];
			assertEquals(0, br.resultCode);

			List<?> results = br.record.getList(ListBin2);
			long size = (Long)results.get(1);
			long val = (Long)results.get(2);

			assertEquals(3, size);
			assertEquals(1, val);
		}
	}

	@Test
	public void batchReadAllBins() {
		Key[] keys = new Key[Size];
		for (int i = 0; i < Size; i++) {
			keys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		Bin bin = new Bin("bin5", "NewValue");

		BatchResults bresults = client.operate(null, null, keys,
			Operation.put(bin),
			Operation.get()
			);

		for (int i = 0; i < bresults.records.length; i++) {
			BatchRecord br = bresults.records[i];
			assertEquals(0, br.resultCode);

			Record r = br.record;

			String s = r.getString(bin.name);
			assertEquals(s, "NewValue");

			Object obj = r.getValue(BinName);
			assertNotNull(obj);
		}
	}

	@Test
	public void batchWriteComplex() {
		Expression wexp1 = Exp.build(Exp.add(Exp.intBin(BinName), Exp.val(1000)));

		Operation[] wops1 = Operation.array(Operation.put(new Bin(BinName2, 100)));
		Operation[] wops2 = Operation.array(ExpOperation.write(BinName3, wexp1, ExpWriteFlags.DEFAULT));
		Operation[] rops1 = Operation.array(Operation.get(BinName2));
		Operation[] rops2 = Operation.array(Operation.get(BinName3));

		BatchWritePolicy wp = new BatchWritePolicy();
		wp.sendKey = true;

		BatchWrite bw1 = new BatchWrite(new Key(args.namespace, args.set, KeyPrefix + 1), wops1);
		BatchWrite bw2 = new BatchWrite(new Key("invalid", args.set, KeyPrefix + 1), wops1);
		BatchWrite bw3 = new BatchWrite(wp, new Key(args.namespace, args.set, KeyPrefix + 6), wops2);
		BatchDelete bd1 = new BatchDelete(new Key(args.namespace, args.set, 10002));

		List<BatchRecord> records = new ArrayList<BatchRecord>();
		records.add(bw1);
		records.add(bw2);
		records.add(bw3);
		records.add(bd1);

		boolean status = client.operate(null, records);
		assertFalse(status);  // "invalid" namespace triggers the false status.

		assertEquals(ResultCode.OK, bw1.resultCode);
		assertBinEqual(bw1.key, bw1.record, BinName2, 0);

		assertEquals(ResultCode.INVALID_NAMESPACE, bw2.resultCode);

		assertEquals(ResultCode.OK, bw3.resultCode);
		assertBinEqual(bw3.key, bw3.record, BinName3, 0);

		assertEquals(ResultCode.OK, bd1.resultCode);

		BatchRead br1 = new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 1), rops1);
		BatchRead br2 = new BatchRead(new Key(args.namespace, args.set, KeyPrefix + 6), rops2);
		BatchRead br3 = new BatchRead(new Key(args.namespace, args.set, 10002), true);

		records.clear();
		records.add(br1);
		records.add(br2);
		records.add(br3);

		status = client.operate(null, records);
		assertFalse(status); // Read of deleted record causes status to be false.

		assertBinEqual(br1.key, br1.record, BinName2, 100);
		assertBinEqual(br2.key, br2.record, BinName3, 1006);
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br3.resultCode);
	}

	@Test
	public void batchDelete() {
		// Define keys
		Key[] keys = new Key[] {
			new Key(args.namespace, args.set, 10000),
			new Key(args.namespace, args.set, 10001)
		};

		// Ensure keys exists
		boolean[] exists = client.exists(null, keys);
		assertTrue(exists[0]);
		assertTrue(exists[1]);

		// Delete keys
		BatchResults br = client.delete(null, null, keys);
		assertTrue(br.status);
		assertEquals(ResultCode.OK, br.records[0].resultCode);
		assertEquals(ResultCode.OK, br.records[1].resultCode);

		// Ensure keys do not exist
		exists = client.exists(null, keys);
		assertFalse(exists[0]);
		assertFalse(exists[1]);
	}

	@Test
	public void batchDeleteSingleNotFound() {
		Key[] keys = new Key[] {
			new Key(args.namespace, args.set, 989299023) // Should be not found.
		};

		BatchResults br = client.delete(null, null, keys);
		assertFalse(br.status);
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br.records[0].resultCode);
	}

	@Test
	public void batchReadTTL() {
		org.junit.Assume.assumeTrue(args.hasTtl);

		// WARNING: This test takes a long time to run due to sleeps.
		// Define keys
		Key key1 = new Key(args.namespace, args.set, 88888);
		Key key2 = new Key(args.namespace, args.set, 88889);

		// Write keys with ttl.
		BatchWritePolicy bwp = new BatchWritePolicy();
		bwp.expiration = 10;
		Key[] keys = new Key[] {key1, key2};
		client.operate(null, bwp, keys, Operation.put(new Bin("a", 1)));

		// Read records before they expire and reset read ttl on one record.
		Util.sleep(8000);
		BatchReadPolicy brp1 = new BatchReadPolicy();
		brp1.readTouchTtlPercent = 80;

		BatchReadPolicy brp2 = new BatchReadPolicy();
		brp2.readTouchTtlPercent = -1;

		BatchRead br1 = new BatchRead(brp1, key1, new String[] {"a"});
		BatchRead br2 = new BatchRead(brp2, key2, new String[] {"a"});

		List<BatchRecord> list = new ArrayList<BatchRecord>();
		list.add(br1);
		list.add(br2);

		boolean rv = client.operate(null, list);

		assertEquals(ResultCode.OK, br1.resultCode);
		assertEquals(ResultCode.OK, br2.resultCode);
		assertTrue(rv);

		// Read records again, but don't reset read ttl.
		Util.sleep(3000);
		brp1.readTouchTtlPercent = -1;
		brp2.readTouchTtlPercent = -1;

		br1 = new BatchRead(brp1, key1, new String[] {"a"});
		br2 = new BatchRead(brp2, key2, new String[] {"a"});

		list.clear();
		list.add(br1);
		list.add(br2);

		rv = client.operate(null, list);

		// Key 2 should have expired.
		assertEquals(ResultCode.OK, br1.resultCode);
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br2.resultCode);
		assertFalse(rv);

		// Read  record after it expires, showing it's gone.
		Util.sleep(8000);
		rv = client.operate(null, list);

		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br1.resultCode);
		assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, br2.resultCode);
		assertFalse(rv);
	}

	private void assertBatchBinEqual(List<BatchRead> list, String binName, int i) {
		BatchRead batch = list.get(i);
		assertBinEqual(batch.key, batch.record, binName, ValuePrefix + (i + 1));
	}

	private void assertBatchRecordExists(List<BatchRead> list, String binName, int i) {
		BatchRead batch = list.get(i);
		assertRecordFound(batch.key, batch.record);
		assertNotEquals(0, batch.record.generation);

		if (args.hasTtl) {
			assertNotEquals(0, batch.record.expiration);
		}
	}
}
