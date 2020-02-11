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

import java.io.IOException;
import java.net.ConnectException;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;

public class AsyncPutGet extends AsyncExample {
	/**
	 * Asynchronously write and read a bin using alternate methods.
	 */
	@Override
	public void runExample(AerospikeClient client, EventLoop eventLoop) {
		Key key = new Key(params.namespace, params.set, "putgetkey");
		Bin bin = new Bin(params.getBinName("putgetbin"), "value");

		runPutGetInline(client, eventLoop, key, bin);
		runPutGetWithRetry(client, eventLoop, key, bin);
	}

	// Inline asynchronous put/get calls.
	private void runPutGetInline(final AerospikeClient client, final EventLoop eventLoop, final Key key, final Bin bin) {

		console.info("Put inline: namespace=%s set=%s key=%s value=%s", key.namespace, key.setName, key.userKey, bin.value);

		client.put(eventLoop, new WriteListener() {
			public void onSuccess(final Key key) {
				try {
					// Write succeeded.  Now call read.
					console.info("Get inline: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

					client.get(eventLoop, new RecordListener() {
						public void onSuccess(final Key key, final Record record) {
							validateBin(key, bin, record, "inline");
						}

						public void onFailure(AerospikeException e) {
							console.error("Failed to get: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
						}
					}, policy, key);
				}
				catch (Exception e) {
					console.error("Failed to get: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
				}
			}

			public void onFailure(AerospikeException e) {
				console.error("Failed to put: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
			}
		}, writePolicy, key, bin);
	}

	// Asynchronous put/get calls with retry.
	private void runPutGetWithRetry(AerospikeClient client, EventLoop eventLoop, Key key, Bin bin) {
		console.info("Put with retry: namespace=%s set=%s key=%s value=%s", key.namespace, key.setName, key.userKey, bin.value);
		client.put(eventLoop, new WriteHandler(client, eventLoop, key, bin), writePolicy, key, bin);
	}

	private class WriteHandler implements WriteListener {
		private final AerospikeClient client;
		private final EventLoop eventLoop;
		private final Key key;
		private final Bin bin;
    	private int failCount = 0;

		public WriteHandler(AerospikeClient client, EventLoop eventLoop, Key key, Bin bin) {
			this.client = client;
			this.eventLoop = eventLoop;
			this.key = key;
			this.bin = bin;
		}

		// Write success callback.
		public void onSuccess(Key key) {
			try {
				// Write succeeded.  Now call read.
				console.info("Get with retry: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);
				client.get(eventLoop, new ReadHandler(client, eventLoop, key, bin), policy, key);
			}
			catch (Exception e) {
				console.error("Failed to get: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
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
                    	client.put(eventLoop, this, writePolicy, key, bin);
                        return;
                    }
                    catch (Exception ex) {
                    	// Fall through to error case.
                    }
            	}
        	}
			console.error("Put failed: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
		}
	}

	private class ReadHandler implements RecordListener {
		private final AerospikeClient client;
		private final EventLoop eventLoop;
		private final Key key;
		private final Bin bin;
    	private int failCount = 0;

		public ReadHandler(AerospikeClient client, EventLoop eventLoop, Key key, Bin bin) {
			this.client = client;
			this.eventLoop = eventLoop;
			this.key = key;
			this.bin = bin;
		}

		// Read success callback.
		public void onSuccess(Key key, Record record) {
			// Verify received bin value is what was written.
			validateBin(key, bin, record, "with retry");
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
                    	client.get(eventLoop, this, policy, key);
                        return;
                    }
                    catch (Exception ex) {
                    	// Fall through to error case.
                    }
            	}
        	}
			console.error("Get failed: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
		}
	}

	private void validateBin(Key key, Bin bin, Record record, String id) {
		Object received = (record == null)? null : record.getValue(bin.name);
		String expected = bin.value.toString();

		if (received != null && received.equals(expected)) {
			console.info("Bin matched %s: namespace=%s set=%s key=%s bin=%s value=%s",
				id, key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Put/Get mismatch: Expected %s. Received %s.", expected, received);
		}
	}
}
