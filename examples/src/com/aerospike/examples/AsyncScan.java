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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.util.Util;

public class AsyncScan extends AsyncExample {

	private int recordCount = 0;

	/**
	 * Asynchronous scan example.
	 */
	@Override
	public void runExample(AerospikeClient client, EventLoop eventLoop) {
		console.info("Asynchronous scan: namespace=" + params.namespace + " set=" + params.set);
		recordCount = 0;
		final long begin = System.currentTimeMillis();
		ScanPolicy policy = new ScanPolicy();
		client.scanAll(eventLoop, new RecordSequenceListener() {

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

		}, policy, params.namespace, params.set);

		// Wait until scan finishes before closing cluster.  This is only necessary
		// when running the async scan example from the command line (which closes the
		// cluster after control is relinquished by this example).

		// The async scan will continue to run after cluster close if the scan was
		// initiated before cluster close.  The problem is cluster close shuts down
		// cluster tending immediately so any data partition migrations will not be
		// received by the client when performing the scan.
		waitTillComplete();
	}
}
