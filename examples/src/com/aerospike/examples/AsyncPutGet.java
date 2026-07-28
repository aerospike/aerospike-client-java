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

import java.io.IOException;
import java.net.ConnectException;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;

public class AsyncPutGet extends AsyncExample {
	/**
	 * Asynchronously write and read a bin using alternate methods.
	 */
	@Override
	public void runExample() {
		Key key = new Key(namespace(), set(), "putgetkey");
		Bin bin = new Bin("putgetbin", "value");

		beginRun();
		try {
			runPutGetInline(key, bin);
		}
		catch (Throwable t) {
			failRun(t);
		}

		beginRun();
		try {
			runPutGetWithRetry(key, bin);
		}
		catch (Throwable t) {
			failRun(t);
		}
	}

	// Inline asynchronous put/get calls.
	private void runPutGetInline(final Key key, final Bin bin) {
		console.info("Put inline: namespace=%s set=%s key=%s value=%s", key.namespace, key.setName, key.userKey, bin.value);

		client().put(eventLoop(), new WriteListener() {
			public void onSuccess(final Key key) {
				try {
					// Write succeeded.  Now call read.
					console.info("Get inline: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

					client().get(eventLoop(), new RecordListener() {
						public void onSuccess(final Key key, final Record record) {
							validateAndComplete(key, bin, record, "inline");
						}

						public void onFailure(AerospikeException e) {
							failRun(e);
						}
					}, readPolicy(), key);
				}
				catch (Throwable t) {
					failRun(t);
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
			}
		}, writePolicy(), key, bin);
	}

	// Asynchronous put/get calls with retry.
	private void runPutGetWithRetry(Key key, Bin bin) {
		console.info("Put with retry: namespace=%s set=%s key=%s value=%s", key.namespace, key.setName, key.userKey, bin.value);
		client().put(eventLoop(), new WriteHandler(key, bin), writePolicy(), key, bin);
	}

	private class WriteHandler implements WriteListener {
		private final Key key;
		private final Bin bin;
		private int failCount = 0;

		public WriteHandler(Key key, Bin bin) {
			this.key = key;
			this.bin = bin;
		}

		// Write success callback.
		public void onSuccess(Key key) {
			try {
				// Write succeeded.  Now call read.
				console.info("Get with retry: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);
				client().get(eventLoop(), new ReadHandler(key, bin), readPolicy(), key);
			}
			catch (Throwable t) {
				failRun(t);
			}
		}

		// Error callback.
		public void onFailure(AerospikeException e) {
			// Retry up to 2 more times.
			if (++failCount <= 2) {
				Throwable t = e.getCause();

				// Check for common socket errors.
				if (t != null && (t instanceof ConnectException || t instanceof IOException)) {
					console.info("Retrying put: " + key.userKey);
					try {
						client().put(eventLoop(), this, writePolicy(), key, bin);
						return;
					}
					catch (Throwable retryFailure) {
						failRun(retryFailure);
						return;
					}
				}
			}
			failRun(e);
		}
	}

	private class ReadHandler implements RecordListener {
		private final Key key;
		private final Bin bin;
		private int failCount = 0;

		public ReadHandler(Key key, Bin bin) {
			this.key = key;
			this.bin = bin;
		}

		// Read success callback.
		public void onSuccess(Key key, Record record) {
			// Verify received bin value is what was written.
			validateAndComplete(key, bin, record, "with retry");
		}

		// Error callback.
		public void onFailure(AerospikeException e) {
			// Retry up to 2 more times.
			if (++failCount <= 2) {
				Throwable t = e.getCause();

				// Check for common socket errors.
				if (t != null && (t instanceof ConnectException || t instanceof IOException)) {
					console.info("Retrying get: " + key.userKey);
					try {
						client().get(eventLoop(), this, readPolicy(), key);
						return;
					}
					catch (Throwable retryFailure) {
						failRun(retryFailure);
						return;
					}
				}
			}
			failRun(e);
		}
	}

	private void validateAndComplete(Key key, Bin bin, Record record, String id) {
		Object received = (record == null) ? null : record.getValue(bin.name);
		Object expected = bin.value.getObject();

		if (expected.equals(received)) {
			console.info("Bin matched %s: namespace=%s set=%s key=%s bin=%s value=%s",
				id, key.namespace, key.setName, key.userKey, bin.name, received);
			completeRun();
		}
		else {
			failRun(new IllegalStateException(
				String.format("Put/Get mismatch: expected=%s received=%s", expected, received)));
		}
	}
}
