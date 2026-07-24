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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log.Level;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.util.Util;

public class AsyncBatch extends AsyncExample {
	private final String keyPrefix = "batchkey";
	private final String valuePrefix = "batchvalue";
	private Key[] sendKeys;
	private String binName;
	private final int size = 8;

	/**
	 * Asynchronous batch examples.
	 */
	@Override
	public void runExample() {
		this.binName = "batchbin";

		initializeKeys();
		beginRun();
		writeRecords();
	}

	private void initializeKeys() throws AerospikeException {
		sendKeys = new Key[size];

		for (int i = 0; i < size; i++) {
			sendKeys[i] = new Key(namespace(), set(), keyPrefix + (i + 1));
		}
	}

	/**
	 * Write records individually.
	 */
	private void writeRecords() {
		WriteHandler handler = new WriteHandler();
		int launched = 0;

		try {
			for (int i = 1; i <= size; i++) {
				Key key = sendKeys[i - 1];
				Bin bin = new Bin(binName, valuePrefix + i);

				console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
					key.namespace, key.setName, key.userKey, bin.name, bin.value);

				launched++;
				client().put(eventLoop(), handler, writePolicy(), key, bin);
			}
		}
		catch (Throwable t) {
			handler.onLaunchFailure(launched, t);
		}
	}

	private class WriteHandler implements WriteListener {
		private int launched = size;
		private int count;
		private Throwable failure;

		private void onLaunchFailure(int launched, Throwable t) {
			this.launched = launched;

			if (failure == null) {
				failure = t;
			}

			if (launched == 0) {
				failRun(failure);
			}
			else if (count == launched) {
				finish();
			}
		}

		public void onSuccess(Key key) {
			// Use non-atomic increment because all writes are performed
			// in the same event loop thread.
			if (++count == launched) {
				finish();
			}
		}

		public void onFailure(AerospikeException e) {
			if (failure == null) {
				failure = e;
			}

			if (++count == launched) {
				finish();
			}
		}

		private void finish() {
			if (failure != null) {
				failRun(failure);
				return;
			}

			try {
				startBatchExistsArray();
				startBatchExistsSequence();
				startBatchGetArray();
				startBatchGetSequence();
				startBatchGetHeaders();
				startBatchReadComplex();
				completeRun();
			}
			catch (Throwable t) {
				failRun(t);
			}
		}
	}

	private void startBatchExistsArray() throws Exception {
		beginRun();

		try {
			batchExistsArray();
		}
		catch (Throwable t) {
			failRun(t);
			throw t;
		}
	}

	private void startBatchExistsSequence() throws Exception {
		beginRun();

		try {
			batchExistsSequence();
		}
		catch (Throwable t) {
			failRun(t);
			throw t;
		}
	}

	private void startBatchGetArray() throws Exception {
		beginRun();

		try {
			batchGetArray();
		}
		catch (Throwable t) {
			failRun(t);
			throw t;
		}
	}

	private void startBatchGetSequence() throws Exception {
		beginRun();

		try {
			batchGetSequence();
		}
		catch (Throwable t) {
			failRun(t);
			throw t;
		}
	}

	private void startBatchGetHeaders() throws Exception {
		beginRun();

		try {
			batchGetHeaders();
		}
		catch (Throwable t) {
			failRun(t);
			throw t;
		}
	}

	private void startBatchReadComplex() throws Exception {
		beginRun();

		try {
			batchReadComplex();
		}
		catch (Throwable t) {
			failRun(t);
			throw t;
		}
	}

	/**
	 * Check existence of records in one batch, receive in one array.
	 */
	private void batchExistsArray() throws Exception {
		client().exists(eventLoop(), new ExistsArrayListener() {
			public void onSuccess(Key[] keys, boolean[] existsArray) {
				Throwable validationFailure = null;

				for (int i = 0; i < existsArray.length; i++) {
					Key key = keys[i];
					boolean exists = existsArray[i];
					console.info("Record: ns=%s set=%s key=%s exists=%s",
						key.namespace, key.setName, key.userKey, exists);

					if (! exists && validationFailure == null) {
						validationFailure = new IllegalStateException("Expected record to exist: " + key.userKey);
					}
				}

				if (validationFailure != null) {
					failRun(validationFailure);
				}
				else if (existsArray.length != size) {
					failRun(new IllegalStateException(
						"Batch exists array size mismatch. Expected " + size + ". Received " + existsArray.length));
				}
				else {
					completeRun();
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
			}
		}, null, sendKeys);
	}

	/**
	 * Check existence of records in one batch, receive one record at a time.
	 */
	private void batchExistsSequence() throws Exception {
		client().exists(eventLoop(), new ExistsSequenceListener() {
			private int count;
			private Throwable failure;

			public void onExists(Key key, boolean exists) {
				console.info("Record: ns=%s set=%s digest=%s exists=%s",
						key.namespace, key.setName, Buffer.bytesToHexString(key.digest), exists);

				count++;

				if (! exists && failure == null) {
					failure = new IllegalStateException("Expected record to exist: " + key.userKey);
				}
			}

			public void onSuccess() {
				if (failure != null) {
					failRun(failure);
				}
				else if (count != size) {
					failRun(new IllegalStateException(
						"Batch exists sequence count mismatch. Expected " + size + ". Received " + count));
				}
				else {
					completeRun();
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
			}
		}, null, sendKeys);
	}

	/**
	 * Read records in one batch, receive in array.
	 */
	private void batchGetArray() throws Exception {
		client().get(eventLoop(), new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				Throwable validationFailure = null;

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

					Object expected = expectedValueForKey(key);

					if (record == null && validationFailure == null) {
						validationFailure = new IllegalStateException("Batch get array record missing: " + key.userKey);
					}
					else if (record != null && ! expected.equals(value) && validationFailure == null) {
						validationFailure = new IllegalStateException(
							"Batch get array mismatch for " + key.userKey + ": expected=" + expected + " received=" + value);
					}
				}

				if (validationFailure != null) {
					failRun(validationFailure);
				}
				else if (records.length != size) {
					failRun(new IllegalStateException(
						"Record size mismatch. Expected " + size + ". Received " + records.length));
				}
				else {
					completeRun();
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
			}
		}, null, sendKeys);
	}

	/**
	 * Read records in one batch call, receive one record at a time.
	 */
	private void batchGetSequence() throws Exception {
		client().get(eventLoop(), new RecordSequenceListener() {
			private int count;
			private Throwable failure;

			public void onRecord(Key key, Record record) {
				Level level = Level.ERROR;
				Object value = null;

				if (record != null) {
					level = Level.INFO;
					value = record.getValue(binName);
				}
				console.write(level, "Record: ns=%s set=%s digest=%s bin=%s value=%s",
					key.namespace, key.setName, Buffer.bytesToHexString(key.digest), binName, value);

				count++;

				Object expected = expectedValueForKey(key);

				if (record == null && failure == null) {
					failure = new IllegalStateException("Batch get sequence record missing: " + key.userKey);
				}
				else if (record != null && ! expected.equals(value) && failure == null) {
					failure = new IllegalStateException(
						"Batch get sequence mismatch for " + key.userKey + ": expected=" + expected + " received=" + value);
				}
			}

			public void onSuccess() {
				if (failure != null) {
					failRun(failure);
				}
				else if (count != size) {
					failRun(new IllegalStateException(
						"Batch get sequence count mismatch. Expected " + size + ". Received " + count));
				}
				else {
					completeRun();
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
			}
		}, null, sendKeys);
	}

	/**
	 * Read record headers in one batch, receive in an array.
	 */
	private void batchGetHeaders() throws Exception {
		client().getHeader(eventLoop(), new RecordArrayListener() {
			public void onSuccess(Key[] keys, Record[] records) {
				Throwable validationFailure = null;

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

					if ((record == null || (record.generation <= 0 && record.expiration <= 0)) && validationFailure == null) {
						validationFailure = new IllegalStateException("Batch header missing for key " + key.userKey);
					}
				}

				if (validationFailure != null) {
					failRun(validationFailure);
				}
				else if (records.length != size) {
					failRun(new IllegalStateException(
						"Header size mismatch. Expected " + size + ". Received " + records.length));
				}
				else {
					completeRun();
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
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
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 1), bins));
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 2), true));
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 3), true));
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 4), false));
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 5), true));
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 6), true));
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 7), bins));

		// This record should be found, but the requested bin will not be found.
		records.add(new BatchRead(new Key(namespace(), set(), keyPrefix + 8), new String[] {"binnotfound"}));

		// This record should not be found.
		records.add(new BatchRead(new Key(namespace(), set(), "keynotfound"), bins));

		// Execute batch.
		client().get(eventLoop(), new BatchListListener() {
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
					failRun(new IllegalStateException(
						"Records found mismatch. Expected 8. Received " + found));
				}
				else {
					completeRun();
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
			}
		}, null, records);
	}

	private String expectedValueForKey(Key key) {
		String userKey = String.valueOf(key.userKey);
		return valuePrefix + userKey.substring(keyPrefix.length());
	}
}
