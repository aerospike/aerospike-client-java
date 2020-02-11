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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log.Level;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.util.Util;

public class AsyncBatch extends AsyncExample {

	private AerospikeClient client;
	private EventLoop eventLoop;
	private final String keyPrefix = "batchkey";
	private final String valuePrefix = "batchvalue";
	private Key[] sendKeys;
	private String binName;
	private final int size = 8;

	/**
	 * Asynchronous batch examples.
	 */
	@Override
	public void runExample(AerospikeClient client, EventLoop eventLoop) {
		this.client = client;
		this.eventLoop = eventLoop;
		this.binName = params.getBinName("batchbin");

		initializeKeys();
		writeRecords();
	}

	private void initializeKeys() throws AerospikeException {
		sendKeys = new Key[size];

		for (int i = 0; i < size; i++) {
			sendKeys[i] = new Key(params.namespace, params.set, keyPrefix + (i + 1));
		}
	}

	/**
	 * Write records individually.
	 */
	private void writeRecords() {
		WriteHandler handler = new WriteHandler(size);

		for (int i = 1; i <= size; i++) {
			Key key = sendKeys[i-1];
			Bin bin = new Bin(binName, valuePrefix + i);

			console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, bin.value);

			client.put(eventLoop, handler, params.writePolicy, key, bin);
		}
	}

	private class WriteHandler implements WriteListener {
		private final int max;
		private AtomicInteger count = new AtomicInteger();

		public WriteHandler(int max) {
			this.max = max;
		}

		public void onSuccess(Key key) {
			int rows = count.incrementAndGet();

			if (rows == max) {
				try {
					// All writes succeeded. Run batch queries in parallel.
					batchExistsArray();
					batchExistsSequence();
					batchGetArray();
					batchGetSequence();
					batchGetHeaders();
					batchReadComplex();
				}
				catch (Exception e) {
					console.error("Batch failed: " + e.getMessage());
				}
			}
		}

		public void onFailure(AerospikeException e) {
			console.error("Put failed: " + e.getMessage());
		}
	}

	/**
	 * Check existence of records in one batch, receive in one array.
	 */
	private void batchExistsArray() throws Exception {
		client.exists(eventLoop, new ExistsArrayListener() {
			public void onSuccess(Key[] keys, boolean[] existsArray) {
				for (int i = 0; i < existsArray.length; i++) {
					Key key = keys[i];
					boolean exists = existsArray[i];
		            console.info("Record: ns=%s set=%s key=%s exists=%s",
		            	key.namespace, key.setName, key.userKey, exists);
		        }
			}

			public void onFailure(AerospikeException e) {
				console.error("Batch exists array failed: " + Util.getErrorMessage(e));
			}
		}, null, sendKeys);
    }

	/**
	 * Check existence of records in one batch, receive one record at a time.
	 */
	private void batchExistsSequence() throws Exception {
		client.exists(eventLoop, new ExistsSequenceListener() {
			public void onExists(Key key, boolean exists) {
		        console.info("Record: ns=%s set=%s digest=%s exists=%s",
		            	key.namespace, key.setName, Buffer.bytesToHexString(key.digest), exists);
			}

			public void onSuccess() {
			}

			public void onFailure(AerospikeException e) {
				console.error("Batch exists sequence failed: " + Util.getErrorMessage(e));
			}
		}, null, sendKeys);
    }

	/**
	 * Read records in one batch, receive in array.
	 */
	private void batchGetArray() throws Exception {
		client.get(eventLoop, new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				for (int i = 0; i < records.length; i++) {
					Key key = keys[i];
					Record record = records[i];
					Level level = Level.ERROR;
					Object value = null;

					if (record != null) {
						level = Level.INFO;
						value = record.getValue(binName);
					}
			        console.write(level, "Record: ns=%s set=%s key=%s bin=%s value=%s",
			            key.namespace, key.setName, key.userKey, binName, value);
		        }

				if (records.length != size) {
		        	console.error("Record size mismatch. Expected %d. Received %d.", size, records.length);
				}
			}

			public void onFailure(AerospikeException e) {
				console.error("Batch get array failed: " + Util.getErrorMessage(e));
			}
		}, null, sendKeys);
    }

	/**
	 * Read records in one batch call, receive one record at a time.
	 */
	private void batchGetSequence() throws Exception {
		client.get(eventLoop, new RecordSequenceListener() {
			public void onRecord(Key key, Record record) {
				Level level = Level.ERROR;
				Object value = null;

				if (record != null) {
					level = Level.INFO;
					value = record.getValue(binName);
				}
		        console.write(level, "Record: ns=%s set=%s digest=%s bin=%s value=%s",
		            key.namespace, key.setName, Buffer.bytesToHexString(key.digest), binName, value);
			}

			public void onSuccess() {
			}

			public void onFailure(AerospikeException e) {
				console.error("Batch get sequence failed: " + Util.getErrorMessage(e));
			}
		}, null, sendKeys);
    }

	/**
	 * Read record headers in one batch, receive in an array.
	 */
	private void batchGetHeaders() throws Exception {
		client.getHeader(eventLoop, new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				for (int i = 0; i < records.length; i++) {
					Key key = keys[i];
					Record record = records[i];
					Level level = Level.ERROR;
					int generation = 0;
					int expiration = 0;

					if (record != null && (record.generation > 0 || record.expiration > 0)) {
						level = Level.INFO;
						generation = record.generation;
						expiration = record.expiration;
					}
			        console.write(level, "Record: ns=%s set=%s key=%s generation=%d expiration=%d",
			            key.namespace, key.setName, key.userKey, generation, expiration);
		        }

				if (records.length != size) {
		        	console.error("Record size mismatch. Expected %d. Received %d.", size, records.length);
				}
			}

			public void onFailure(AerospikeException e) {
				console.error("Batch get headers failed: " + Util.getErrorMessage(e));
			}
		}, null, sendKeys);
    }

	/**
	 * Read records with varying namespaces, bin names and read types in one batch.
	 * This requires Aerospike Server version >= 3.6.0
	 */
	private void batchReadComplex() throws Exception {
		// Batch gets into one call.
		// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
		String[] bins = new String[] {binName};
		List<BatchRead> records = new ArrayList<BatchRead>();
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 1), bins));
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 2), true));
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 3), true));
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 4), false));
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 5), true));
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 6), true));
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 7), bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(new Key(params.namespace, params.set, keyPrefix + 8), new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(new Key(params.namespace, params.set, "keynotfound"), bins));

		// Execute batch.
		client.get(eventLoop, new BatchListListener() {
			public void onSuccess(List<BatchRead> records) {
				// Show results.
				int found = 0;
				for (BatchRead record : records) {
					Key key = record.key;
					Record rec = record.record;

					if (rec != null) {
						found++;
						console.info("Record: ns=%s set=%s key=%s bin=%s value=%s",
							key.namespace, key.setName, key.userKey, binName, rec.getValue(binName));
					}
					else {
						console.info("Record not found: ns=%s set=%s key=%s bin=%s",
							key.namespace, key.setName, key.userKey, binName);
					}
				}

				if (found != 8) {
					console.error("Records found mismatch. Expected %d. Received %d.", 8, found);
				}
			}

			public void onFailure(AerospikeException e) {
				console.error("Batch read complex failed: " + Util.getErrorMessage(e));
			}
		}, null, records);
	}
}
