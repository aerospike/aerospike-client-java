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
package com.aerospike.test.async;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
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
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

public class TestAsyncBatch extends TestAsync {
	private static final String BinName = "batchbin";
	private static final String BinName2 = "batchbin2";
	private static final String BinName3 = "batchbin3";
	private static final String ListBin = "listbin";
	private static final String ListBin2 = "listbin2";
	private static final String ListBin3 = "listbin3";
	private static final String KeyPrefix = "batchkey";
	private static final String ValuePrefix = "batchvalue";
	private static final int Size = 8;
	private static Key[] sendKeys;
	private static Key[] deleteKeys;
	private static Key[] deleteKeysSequence;

	@BeforeClass
	public static void initialize() {
		sendKeys = new Key[Size];

		for (int i = 0; i < Size; i++) {
			sendKeys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		deleteKeys = new Key[2];
		deleteKeys[0] = new Key(args.namespace, args.set, 10000);
		deleteKeys[1] = new Key(args.namespace, args.set, 10001);

		deleteKeysSequence = new Key[2];
		deleteKeysSequence[0] = new Key(args.namespace, args.set, 11000);
		deleteKeysSequence[1] = new Key(args.namespace, args.set, 11001);

		AsyncMonitor monitor = new AsyncMonitor();

		WriteListener listener = new WriteListener() {
			private int count = 0;

			public void onSuccess(final Key key) {
				// Use non-atomic increment because all writes are performed
				// in the same event loop thread.
				if (++count == Size + 5) {
					monitor.notifyComplete();
				}
			}

			public void onFailure(AerospikeException e) {
				monitor.setError(e);
				monitor.notifyComplete();
			}
		};

		WritePolicy policy = new WritePolicy();

		if (args.hasTtl) {
			policy.expiration = 2592000;
		}

		for (int i = 1; i <= Size; i++) {
			Key key = sendKeys[i-1];
			Bin bin = new Bin(BinName, ValuePrefix + i);

			List<Integer> list = new ArrayList<Integer>();

			for (int j = 0; j < i; j++) {
				list.add(j * i);
			}

			List<Integer> list2 = new ArrayList<Integer>();

			for (int j = 0; j < 2; j++) {
				list2.add(j);
			}

			List<Integer> list3 = new ArrayList<Integer>();

			for (int j = 0; j < 2; j++) {
				list3.add(j);
			}

			Bin listBin = new Bin(ListBin, list);
			Bin listBin2 = new Bin(ListBin2, list2);
			Bin listBin3 = new Bin(ListBin3, list3);

			if (i != 6) {
				client.put(eventLoop, listener, policy, key, bin, listBin, listBin2, listBin3);
			}
			else {
				client.put(eventLoop, listener, policy, key, new Bin(BinName, i), listBin, listBin2, listBin3);
			}
		}

		// Add records that will eventually be deleted.
		client.put(eventLoop, listener, policy, deleteKeys[0], new Bin(BinName, 10000));
		client.put(eventLoop, listener, policy, deleteKeys[1], new Bin(BinName, 10001));
		client.put(eventLoop, listener, policy, new Key(args.namespace, args.set, 10002), new Bin(BinName, 10002));
		client.put(eventLoop, listener, policy, deleteKeysSequence[0], new Bin(BinName, 11000));
		client.put(eventLoop, listener, policy, deleteKeysSequence[1], new Bin(BinName, 11001));

		monitor.waitTillComplete();
	}

	@Test
	public void asyncBatchExistsArray() {
		client.exists(eventLoop, new ExistsArrayListener() {
			public void onSuccess(Key[] keys, boolean[] existsArray) {
				for (int i = 0; i < existsArray.length; i++) {
					if (! assertEquals(true, existsArray[i])) {
						break;
					}
				}
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys);

		waitTillComplete();
	}

	@Test
	public void asyncBatchExistsSequence() throws Exception {
		client.exists(eventLoop, new ExistsSequenceListener() {
			public void onExists(Key key, boolean exists) {
				assertEquals(true, exists);
			}

			public void onSuccess() {
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys);

		waitTillComplete();
	}

	@Test
	public void asyncBatchGetArray() throws Exception {
		client.get(eventLoop, new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				if (assertEquals(Size, records.length)) {
					for (int i = 0; i < records.length; i++) {
						if (i != 5) {
							if (! assertBinEqual(keys[i], records[i], BinName, ValuePrefix + (i + 1))) {
								break;
							}
						}
						else {
							if (! assertBinEqual(keys[i], records[i], BinName, i + 1)) {
								break;
							}
						}
					}
				}
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys);

		waitTillComplete();
	}

	@Test
	public void asyncBatchGetArrayBinName() throws Exception {
		client.get(eventLoop, new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				if (assertEquals(Size, records.length)) {
					for (int i = 0; i < records.length; i++) {
						if (i != 5) {
							if (! assertBinEqual(keys[i], records[i], BinName, ValuePrefix + (i + 1))) {
								break;
							}
						}
						else {
							if (! assertBinEqual(keys[i], records[i], BinName, i + 1)) {
								break;
							}
						}
					}
				}
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys, BinName);

		waitTillComplete();
	}

	@Test
	public void asyncBatchGetSequence() throws Exception {
		client.get(eventLoop, new RecordSequenceListener() {
			public void onRecord(Key key, Record record) {
				if (assertRecordFound(key, record))  {
					Object value = record.getValue(BinName);
					assertNotNull(value);
				}
			}

			public void onSuccess() {
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys);

		waitTillComplete();
	}

	@Test
	public void asyncBatchGetSequenceBinName() throws Exception {
		client.get(eventLoop, new RecordSequenceListener() {
			public void onRecord(Key key, Record record) {
				if (assertRecordFound(key, record))  {
					Object value = record.getValue(BinName);
					assertNotNull(value);
				}
			}

			public void onSuccess() {
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys, BinName);

		waitTillComplete();
	}

	@Test
	public void asyncBatchGetHeaders() throws Exception {
		client.getHeader(eventLoop, new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				if (assertEquals(Size, records.length)) {
					for (int i = 0; i < records.length; i++) {
						Record record = records[i];

						if (! assertRecordFound(keys[i], record)) {
							break;
						}

						if (! assertGreaterThanZero(record.generation)) {
							break;
						}

						if (args.hasTtl && !assertGreaterThanZero(record.expiration)) {
							break;
						}
					}
				}
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys);

		waitTillComplete();
	}

	@Test
	public void asyncBatchGetHeadersSeq() throws Exception {
		client.getHeader(eventLoop, new RecordSequenceListener() {
			int count;

			public void onRecord(Key key, Record record) {
				count++;

				int index = getKeyIndex(key);

				if (!assertTrue(index >= 0)) {
					notifyComplete();
					return;
				}

				if (! assertRecordFound(sendKeys[index], record)) {
					notifyComplete();
					return;
				}

				if (! assertGreaterThanZero(record.generation)) {
					notifyComplete();
					return;
				}

				if (args.hasTtl && !assertGreaterThanZero(record.expiration)) {
					notifyComplete();
					return;
				}
			}

			public void onSuccess() {
				assertEquals(Size, count);
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}

			private int getKeyIndex(Key key) {
				for (int i = 0; i < sendKeys.length; i++) {
					if (key == sendKeys[i]) {
						return i;
					}
				}
				return -1;
			}
		}, null, sendKeys);

		waitTillComplete();
	}

	@Test
	public void asyncBatchReadComplex() throws Exception {
		// Batch gets into one call.
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
		client.get(eventLoop, new BatchListListener() {
			public void onSuccess(List<BatchRead> records) {
				// Show results.
				int found = 0;
				int count = 0;
				for (BatchRead record : records) {
					Record rec = record.record;
					count++;

					if (rec != null) {
						found++;

						if (count != 4 && count != 6 && count <= 7) {
							Object value = rec.getValue(BinName);

							if (!assertEquals(ValuePrefix + count, value)) {
								notifyComplete();
								return;
							}
						}
						else if (count == 6) {
							int value = rec.getInt(BinName);

							if (!assertEquals(48, value)) {
								notifyComplete();
								return;
							}
						}
						else {
							Object value = rec.getValue(BinName);

							if (!assertNull(value)) {
								notifyComplete();
								return;
							}
						}
					}
				}

				assertEquals(8, found);
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, records);

		waitTillComplete();
	}

	@Test
	public void asyncBatchReadComplexSeq() throws Exception {
		Expression exp = Exp.build(Exp.mul(Exp.intBin(BinName), Exp.val(8)));
		Operation[] ops = Operation.array(ExpOperation.read(BinName, exp, ExpReadFlags.DEFAULT));

		String[] bins = new String[] {BinName};

		Key[] keys = new Key[9];
		keys[0] = new Key(args.namespace, args.set, KeyPrefix + 1);
		keys[1] = new Key(args.namespace, args.set, KeyPrefix + 2);
		keys[2] = new Key(args.namespace, args.set, KeyPrefix + 3);
		keys[3] = new Key(args.namespace, args.set, KeyPrefix + 4);
		keys[4] = new Key(args.namespace, args.set, KeyPrefix + 5);
		keys[5] = new Key(args.namespace, args.set, KeyPrefix + 6);
		keys[6] = new Key(args.namespace, args.set, KeyPrefix + 7);
		keys[7] = new Key(args.namespace, args.set, KeyPrefix + 8);
		keys[8] = new Key(args.namespace, args.set, "keynotfound");

		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(keys[0], bins));
		records.add(new BatchRead(keys[1], true));
		records.add(new BatchRead(keys[2], true));
		records.add(new BatchRead(keys[3], false));
		records.add(new BatchRead(keys[4], true));
		records.add(new BatchRead(keys[5], ops));
		records.add(new BatchRead(keys[6], bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(keys[7], new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(keys[8], bins));

		// Execute batch.
		client.get(eventLoop, new BatchSequenceListener() {
			private int found;

			public void onRecord(BatchRead record) {
				Record rec = record.record;

				if (rec != null) {
					found++;

					int index = getKeyIndex(record.key);

					if (!assertTrue(index >= 0)) {
						notifyComplete();
						return;
					}

					if (index != 3 && index != 5 && index <= 6) {
						Object value = rec.getValue(BinName);

						if (!assertEquals(ValuePrefix + (index+1), value)) {
							notifyComplete();
							return;
						}
					}
					else if (index == 5) {
						int value = rec.getInt(BinName);

						if (!assertEquals(48, value)) {
							notifyComplete();
							return;
						}
					}
					else {
						Object value = rec.getValue(BinName);

						if (!assertNull(value)) {
							notifyComplete();
							return;
						}
					}
				}
			}

			private int getKeyIndex(Key key) {
				for (int i = 0; i < keys.length; i++) {
					if (key == keys[i]) {
						return i;
					}
				}
				return -1;
			}

			public void onSuccess() {
				assertEquals(8, found);
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, records);

		waitTillComplete();
	}

	@Test
	public void asyncBatchListReadOperate() throws Exception {
		client.get(eventLoop, new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				if (assertEquals(Size, records.length)) {
					for (int i = 0; i < records.length; i++) {
						Record record = records[i];
						List<?> results = record.getList(ListBin);
						long size = (Long)results.get(0);
						long val = (Long)results.get(1);

						assertEquals(i + 1, size);
						assertEquals(i * (i + 1), val);
					}
				}
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, sendKeys,
			ListOperation.size(ListBin),
			ListOperation.getByIndex(ListBin, -1, ListReturnType.VALUE)
		);

		waitTillComplete();
	}

	@Test
	public void asyncBatchListReadOperateSeq() throws Exception {
		client.get(eventLoop, new RecordSequenceListener() {
			int count;

			public void onRecord(Key key, Record record) {
				count++;

				int index = getKeyIndex(key);

				if (!assertTrue(index >= 0)) {
					notifyComplete();
					return;
				}

				List<?> results = record.getList(ListBin);
				long size = (Long)results.get(0);
				long val = (Long)results.get(1);

				if (! assertEquals(index + 1, size)) {
					notifyComplete();
					return;
				}

				if (! assertEquals(index * (index + 1), val)) {
					notifyComplete();
					return;
				}
			}

			public void onSuccess() {
				assertEquals(Size, count);
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}

			private int getKeyIndex(Key key) {
				for (int i = 0; i < sendKeys.length; i++) {
					if (key == sendKeys[i]) {
						return i;
					}
				}
				return -1;
			}
		}, null, sendKeys,
			ListOperation.size(ListBin),
			ListOperation.getByIndex(ListBin, -1, ListReturnType.VALUE)
		);

		waitTillComplete();
	}

	@Test
	public void asyncBatchListWriteOperate() {
		client.operate(eventLoop, new BatchRecordArrayListener() {
			public void onSuccess(BatchRecord[] records, boolean status) {
				assertEquals(true, status);

				if (assertEquals(Size, records.length)) {
					for (int i = 0; i < records.length; i++) {
						Record record = records[i].record;
						List<?> results = record.getList(ListBin2);
						long size = (Long)results.get(1);
						long val = (Long)results.get(2);

						assertEquals(3, size);
						assertEquals(1, val);
					}
				}
				notifyComplete();
			}

			public void onFailure(BatchRecord[] records, AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, null, sendKeys,
			ListOperation.insert(ListBin2, 0, Value.get(1000)),
			ListOperation.size(ListBin2),
			ListOperation.getByIndex(ListBin2, -1, ListReturnType.VALUE)
		);

		waitTillComplete();
	}

	@Test
	public void asyncBatchSeqListWriteOperate() {
		client.operate(eventLoop, new BatchRecordSequenceListener() {
			int count = 0;

			public void onRecord(BatchRecord record, int index) {
				Record rec = record.record;

				if (assertNotNull(rec)) {
					List<?> results = rec.getList(ListBin3);
					long size = (Long)results.get(1);
					long val = (Long)results.get(2);

					assertEquals(3, size);
					assertEquals(1, val);
					count++;
				}
			}

			public void onSuccess() {
				assertEquals(Size, count);
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, null, sendKeys,
			ListOperation.insert(ListBin3, 0, Value.get(1000)),
			ListOperation.size(ListBin3),
			ListOperation.getByIndex(ListBin3, -1, ListReturnType.VALUE)
		);

		waitTillComplete();
	}

	@Test
	public void asyncBatchWriteComplex() {
		Expression wexp1 = Exp.build(Exp.add(Exp.intBin(BinName), Exp.val(1000)));

		Operation[] ops1 = Operation.array(
			Operation.put(new Bin(BinName2, 100)),
			Operation.get(BinName2));

		Operation[] ops2 = Operation.array(
			ExpOperation.write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
			Operation.get(BinName3));

		List<BatchRecord> records = new ArrayList<BatchRecord>();
		records.add(new BatchWrite(new Key(args.namespace, args.set, KeyPrefix + 1), ops1));
		records.add(new BatchWrite(new Key(args.namespace, args.set, KeyPrefix + 6), ops2));

		client.operate(eventLoop, new BatchOperateListListener() {
			public void onSuccess(List<BatchRecord> records, boolean status) {
				try {
					assertEquals(true, status);

					BatchRecord r = records.get(0);
					assertBatchBinEqual(r, BinName2, 100);

					r = records.get(1);
					assertBatchBinEqual(r, BinName3, 1006);
				}
				catch (Throwable e) {
					setError(new AerospikeException(e));
				}
				finally {
					notifyComplete();
				}
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, records);

		waitTillComplete();
	}

	@Test
	public void asyncBatchSeqWriteComplex() {
		Expression wexp1 = Exp.build(Exp.add(Exp.intBin(BinName), Exp.val(1000)));

		Operation[] ops1 = Operation.array(
			Operation.put(new Bin(BinName2, 100)),
			Operation.get(BinName2));

		Operation[] ops2 = Operation.array(
			ExpOperation.write(BinName3, wexp1, ExpWriteFlags.DEFAULT),
			Operation.get(BinName3));

		List<BatchRecord> records = new ArrayList<BatchRecord>();
		records.add(new BatchWrite(new Key(args.namespace, args.set, KeyPrefix + 1), ops1));
		records.add(new BatchWrite(new Key(args.namespace, args.set, KeyPrefix + 6), ops2));
		records.add(new BatchDelete(new Key(args.namespace, args.set, 10002)));

		client.operate(eventLoop, new BatchRecordSequenceListener() {
			int count = 0;

			public void onRecord(BatchRecord r, int index) {
				count++;

				switch (index) {
				case 0:
					assertBatchBinEqual(r, BinName2, 100);
					break;

				case 1:
					assertBatchBinEqual(r, BinName3, 1006);
					break;

				case 2:
					assertEquals(ResultCode.OK, r.resultCode);
					break;

				default:
					setError(new Exception("Unexpected batch index: " + index));
				}
			}

			public void onSuccess() {
				assertEquals(3, count);
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, records);

		waitTillComplete();
	}

	private boolean assertBatchBinEqual(BatchRecord r, String binName, int expected) {
		try {
			if (! assertRecordFound(r.key, r.record)) {
				return false;
			}

			List<?> list = r.record.getList(binName);
			Object obj = list.get(0);

			if (obj != null) {
				setError(new Exception("Data mismatch: Expected null. Received " + obj));
				return false;
			}

			long val = (Long)list.get(1);

			if (val != expected) {
				setError(new Exception("Data mismatch: Expected " + expected + ". Received " + val));
				return false;
			}
			return true;
		}
		catch (Throwable e) {
			setError(new AerospikeException(e));
			return false;
		}
	}

	@Test
	public void asyncBatchDelete() {
		// Ensure keys exists
		client.exists(eventLoop, new ExistsArrayListener() {
			public void onSuccess(Key[] keys, boolean[] exists) {
				assertEquals(true, exists[0]);
				assertEquals(true, exists[1]);

				// Delete keys
				client.delete(eventLoop, new BatchRecordArrayListener() {
					public void onSuccess(BatchRecord[] records, boolean status) {
						assertEquals(true, status);

						// Ensure keys do not exist
						client.exists(eventLoop, new ExistsArrayListener() {
							public void onSuccess(Key[] keys, boolean[] exists) {
								assertEquals(false, exists[0]);
								assertEquals(false, exists[1]);
								notifyComplete();
							}

							public void onFailure(AerospikeException e) {
								setError(e);
								notifyComplete();
							}
						}, null, deleteKeys);
					}

					public void onFailure(BatchRecord[] records, AerospikeException e) {
						setError(e);
						notifyComplete();
					}
				}, null, null, keys);
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, deleteKeys);

		waitTillComplete();
	}

	@Test
	public void asyncBatchDeleteSequence() {
		// Ensure keys exists
		client.exists(eventLoop, new ExistsArrayListener() {
			public void onSuccess(Key[] keys, boolean[] exists) {
				assertEquals(true, exists[0]);
				assertEquals(true, exists[1]);

				// Delete keys
				client.delete(eventLoop, new BatchRecordSequenceListener() {
					public void onRecord(BatchRecord record, int index) {
						assertEquals(ResultCode.OK, record.resultCode);
						assertTrue(index <= 1);
					}

					public void onSuccess() {
						// Ensure keys do not exist
						client.exists(eventLoop, new ExistsArrayListener() {
							public void onSuccess(Key[] keys, boolean[] exists) {
								assertEquals(false, exists[0]);
								assertEquals(false, exists[1]);
								notifyComplete();
							}

							public void onFailure(AerospikeException e) {
								setError(e);
								notifyComplete();
							}
						}, null, deleteKeysSequence);
					}

					public void onFailure(AerospikeException e) {
						setError(e);
						notifyComplete();
					}
				}, null, null, keys);
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, deleteKeysSequence);

		waitTillComplete();
	}
}
