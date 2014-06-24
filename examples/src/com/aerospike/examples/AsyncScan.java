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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.util.Util;

public class AsyncScan extends AsyncExample {
		
	private int recordCount = 0;
	private boolean completed;

	public AsyncScan(Console console) {
		super(console);
	}

	/**
	 * Asynchronous scan example.
	 */
	@Override
	public void runExample(AsyncClient client, Parameters params) throws Exception {
		console.info("Asynchronous scan: namespace=" + params.namespace + " set=" + params.set);
		recordCount = 0;
		final long begin = System.currentTimeMillis();
		ScanPolicy policy = new ScanPolicy();
		client.scanAll(policy, new RecordSequenceListener() {

			@Override
			public void onRecord(Key key, Record record) throws AerospikeException {
				recordCount++;

				if ((recordCount % 10000) == 0) {
					console.info("Records " + recordCount);
				}
			}

			@Override
			public void onSuccess() {
				long end = System.currentTimeMillis();
				double seconds =  (double)(end - begin) / 1000.0;
				console.info("Total records returned: " + recordCount);
				console.info("Elapsed time: " + seconds + " seconds");
				double performance = Math.round((double)recordCount / seconds);
				console.info("Records/second: " + performance);
				
				notifyComplete();
			}

			@Override
			public void onFailure(AerospikeException e) {
				console.error("Scan failed: " + Util.getErrorMessage(e));
				notifyComplete();
			} 
			
		}, params.namespace, params.set);
		
		waitTillComplete();
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

	private synchronized void notifyComplete() {
		completed = true;
		super.notify();
	}
}
