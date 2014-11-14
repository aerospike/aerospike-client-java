/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class AsyncPutGet extends AsyncExample {
	
	private boolean completed;
	
	public AsyncPutGet(Console console) {
		super(console);
	}

	/**
	 * Asynchronously write and read a bin using alternate methods.
	 */
	@Override
	public void runExample(AsyncClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "putgetkey");
		Bin bin = new Bin(params.getBinName("putgetbin"), "value");

		runPutGetInline(client, params, key, bin);
		waitTillComplete();
		completed = false;
		runPutGetWithRetry(client, params, key, bin);
		waitTillComplete();
	}
	
	// Inline asynchronous put/get calls.
	private void runPutGetInline(final AsyncClient client, final Parameters params, final Key key, final Bin bin) throws AerospikeException {
		
		console.info("Put: namespace=%s set=%s key=%s value=%s", key.namespace, key.setName, key.userKey, bin.value);
		
		client.put(params.writePolicy, new WriteListener() {
			public void onSuccess(final Key key) {
				try {
					// Write succeeded.  Now call read.
					console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

					client.get(params.policy, new RecordListener() {
						public void onSuccess(final Key key, final Record record) {
							validateBin(key, bin, record);
							notifyCompleted();
						}
						
						public void onFailure(AerospikeException e) {
							console.error("Failed to get: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
							notifyCompleted();
						}
					}, key);
				}
				catch (Exception e) {				
					console.error("Failed to get: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
				}
			}
			
			public void onFailure(AerospikeException e) {
				console.error("Failed to put: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
				notifyCompleted();
			}
		}, key, bin);		
	}	

	// Asynchronous put/get calls with retry.
	private void runPutGetWithRetry(AsyncClient client, Parameters params, Key key, Bin bin) throws Exception {
		console.info("Put: namespace=%s set=%s key=%s value=%s", key.namespace, key.setName, key.userKey, bin.value);
		client.put(params.writePolicy, new WriteHandler(client, params.writePolicy, key, bin), key, bin);
	}
	
	private class WriteHandler implements WriteListener {
		private final AsyncClient client;
		private final WritePolicy policy;
		private final Key key;
		private final Bin bin;
    	private int failCount = 0;
		
		public WriteHandler(AsyncClient client, WritePolicy policy, Key key, Bin bin) {
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.bin = bin;
		}
		
		// Write success callback.
		public void onSuccess(Key key) {
			try {
				// Write succeeded.  Now call read.
				console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);
				client.get(policy, new ReadHandler(client, policy, key, bin), key);
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
                    	client.put(policy, this, key, bin);
                        return;
                    }
                    catch (Exception ex) {
                    	// Fall through to error case.
                    }
            	}
        	}
			console.error("Put failed: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
			notifyCompleted();
		}
	}

	private class ReadHandler implements RecordListener {
		private final AsyncClient client;
		private final Policy policy;
		private final Key key;
		private final Bin bin;
    	private int failCount = 0;
		
		public ReadHandler(AsyncClient client, Policy policy, Key key, Bin bin) {
			this.client = client;
			this.policy = policy;
			this.key = key;
			this.bin = bin;
		}
				
		// Read success callback.
		public void onSuccess(Key key, Record record) {
			// Verify received bin value is what was written.
			validateBin(key, bin, record);
			notifyCompleted();
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
                    	client.get(policy, this, key);
                        return;
                    }
                    catch (Exception ex) {
                    	// Fall through to error case.
                    }
            	}
        	}
			console.error("Get failed: namespace=%s set=%s key=%s exception=%s", key.namespace, key.setName, key.userKey, e.getMessage());
			notifyCompleted();
		}
	}

	private void validateBin(Key key, Bin bin, Record record) {
		Object received = (record == null)? null : record.getValue(bin.name);
		String expected = bin.value.toString();
		
		if (received != null && received.equals(expected)) {
			console.info("Bin matched: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Put/Get mismatch: Expected %s. Received %s.", expected, received);
		}
	}
	
	private synchronized void waitTillComplete() {
		while (! completed) {
			try {
				super.wait();
			}
			catch (InterruptedException ie) {
			}
		}
	}

	private synchronized void notifyCompleted() {
		completed = true;
		super.notify();
	}
}
