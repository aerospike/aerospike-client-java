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
package com.aerospike.test.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

public class TestAsyncBatch extends TestAsync {
	private static final String keyPrefix = "batchkey";
	private static final String valuePrefix = "batchvalue";
	private static final String binName = args.getBinName("batchbin");
	private static final int size = 8;
	private static Key[] sendKeys;

	@BeforeClass
	public static void initialize() {
		sendKeys = new Key[size];

		for (int i = 0; i < size; i++) {
			sendKeys[i] = new Key(args.namespace, args.set, keyPrefix + (i + 1));
		}

		AsyncMonitor monitor = new AsyncMonitor();
		WriteHandler handler = new WriteHandler(monitor, size);

		WritePolicy policy = new WritePolicy();
		policy.expiration = 2592000;

		for (int i = 1; i <= size; i++) {
			Key key = sendKeys[i-1];
			Bin bin = new Bin(binName, valuePrefix + i);
			client.put(eventLoop, handler, policy, key, bin);
		}
		monitor.waitTillComplete();
	}

	private static class WriteHandler implements WriteListener {
		private AsyncMonitor monitor;
		private final int max;
		private AtomicInteger count = new AtomicInteger();

		public WriteHandler(AsyncMonitor monitor, int max) {
			this.monitor = monitor;
			this.max = max;
		}

		public void onSuccess(Key key) {
			int rows = count.incrementAndGet();

			if (rows == max) {
				monitor.notifyComplete();
			}
		}

		public void onFailure(AerospikeException e) {
			monitor.setError(e);
			monitor.notifyComplete();
		}
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
				if (assertEquals(size, records.length)) {
					for (int i = 0; i < records.length; i++) {
						if (! assertBinEqual(keys[i], records[i], binName, valuePrefix + (i + 1))) {
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
	public void asyncBatchGetSequence() throws Exception {
		client.get(eventLoop, new RecordSequenceListener() {
			public void onRecord(Key key, Record record) {
				if (assertRecordFound(key, record))  {
					Object value = record.getValue(binName);
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
				if (assertEquals(size, records.length)) {
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

						Object value = rec.getValue(binName);

						if (count != 4 && count <= 7) {
							if (!assertEquals(valuePrefix + count, value)) {
								notifyComplete();
								return;
							}
						}
						else {
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
}
