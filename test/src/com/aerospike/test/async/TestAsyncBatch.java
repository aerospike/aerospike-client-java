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
package com.aerospike.test.async;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
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
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

public class TestAsyncBatch extends TestAsync {
	private static final String BinName = "batchbin";
	private static final String ListBin = "listbin";
	private static final String KeyPrefix = "batchkey";
	private static final String ValuePrefix = "batchvalue";
	private static final int Size = 8;
	private static Key[] sendKeys;

	@BeforeClass
	public static void initialize() {
		sendKeys = new Key[Size];

		for (int i = 0; i < Size; i++) {
			sendKeys[i] = new Key(args.namespace, args.set, KeyPrefix + (i + 1));
		}

		AsyncMonitor monitor = new AsyncMonitor();

		WriteListener listener = new WriteListener() {
			private int count = 0;

			public void onSuccess(final Key key) {
				// Use non-atomic increment because all writes are performed
				// in the same event loop thread.
				if (++count == Size) {
					monitor.notifyComplete();
				}
			}

			public void onFailure(AerospikeException e) {
				monitor.setError(e);
				monitor.notifyComplete();
			}
		};

		WritePolicy policy = new WritePolicy();
		policy.expiration = 2592000;

		for (int i = 1; i <= Size; i++) {
			Key key = sendKeys[i-1];
			Bin bin = new Bin(BinName, ValuePrefix + i);

			List<Integer> list = new ArrayList<Integer>();

			for (int j = 0; j < i; j++) {
				list.add(j * i);
			}

			Bin listBin = new Bin(ListBin, list);

			if (i != 6) {
				client.put(eventLoop, listener, policy, key, bin, listBin);
			}
			else {
				client.put(eventLoop, listener, policy, key, new Bin(BinName, i), listBin);
			}
		}
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

						if (! assertGreaterThanZero(record.expiration)) {
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
	public void asyncBatchListOperate() throws Exception {
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
}
