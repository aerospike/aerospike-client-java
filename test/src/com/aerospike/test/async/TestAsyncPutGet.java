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

import java.io.IOException;
import java.net.ConnectException;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class TestAsyncPutGet extends TestAsync {
	private static final String binName = args.getBinName("putgetbin");

	@Test
	public void asyncPutGetInline() {
		final Key key = new Key(args.namespace, args.set, "putgetkey1");
		final Bin bin = new Bin(binName, "value");

		client.put(eventLoop, new WriteListener() {
			public void onSuccess(final Key key) {
				try {
					// Write succeeded.  Now call read.
					client.get(eventLoop, new RecordListener() {
						public void onSuccess(final Key key, final Record record) {
							assertBinEqual(key, record, bin);
							notifyComplete();
						}

						public void onFailure(AerospikeException e) {
							setError(e);
							notifyComplete();
						}
					}, null, key);
				}
				catch (Exception e) {
					setError(e);
					notifyComplete();
				}
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}
		}, null, key, bin);

		waitTillComplete();
	}

	@Test
	public void asyncPutGetWithRetry() {
		final Key key = new Key(args.namespace, args.set, "putgetkey2");
		final Bin bin = new Bin(binName, "value");
		client.put(eventLoop, new WriteHandler(client, null, key, bin), null, key, bin);
		waitTillComplete();
	}

	private class WriteHandler implements WriteListener {
		private final AerospikeClient client;
		private final WritePolicy policy;
		private final Key key;
		private final Bin bin;
    	private int failCount = 0;

		public WriteHandler(AerospikeClient client, WritePolicy policy, Key key, Bin bin) {
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.bin = bin;
		}

		// Write success callback.
		public void onSuccess(Key key) {
			try {
				// Write succeeded.  Now call read.
				client.get(eventLoop, new ReadHandler(client, policy, key, bin), policy, key);
			}
			catch (Exception e) {
				setError(e);
				notifyComplete();
			}
		}

		// Error callback.
		public void onFailure(AerospikeException e) {
		   // Retry up to 2 more times.
           if (++failCount <= 2) {
            	Throwable t = e.getCause();

            	// Check for common socket errors.
            	if (t != null && (t instanceof ConnectException || t instanceof IOException)) {
                    try {
                    	client.put(eventLoop, this, policy, key, bin);
                        return;
                    }
                    catch (Exception ex) {
                    	// Fall through to error case.
                    }
            	}
        	}
			setError(e);
			notifyComplete();
		}
	}

	private class ReadHandler implements RecordListener {
		private final AerospikeClient client;
		private final Policy policy;
		private final Key key;
		private final Bin bin;
    	private int failCount = 0;

		public ReadHandler(AerospikeClient client, Policy policy, Key key, Bin bin) {
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.bin = bin;
		}

		// Read success callback.
		public void onSuccess(Key key, Record record) {
			// Verify received bin value is what was written.
			assertBinEqual(key, record, bin);
			notifyComplete();
		}

		// Error callback.
		public void onFailure(AerospikeException e) {
			// Retry up to 2 more times.
			if (++failCount <= 2) {
            	Throwable t = e.getCause();

            	// Check for common socket errors.
            	if (t != null && (t instanceof ConnectException || t instanceof IOException)) {
                    try {
                    	client.get(eventLoop, this, policy, key);
                        return;
                    }
                    catch (Exception ex) {
                    	// Fall through to error case.
                    }
            	}
        	}
			setError(e);
			notifyComplete();
		}
	}
}
