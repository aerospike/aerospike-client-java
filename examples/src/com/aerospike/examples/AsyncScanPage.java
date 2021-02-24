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
package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.util.Util;

public class AsyncScanPage extends AsyncExample {
	private static final String binName = "bin";
	private static final String setName = "apage";
	private static final int size = 50;

	/**
	 * Asynchronous scan example.
	 */
	@Override
	public void runExample(AerospikeClient client, EventLoop eventLoop) {
		console.info("Write " + size + " records.");

		WriteListener listener = new WriteListener() {
			private int count = 0;

			public void onSuccess(final Key key) {
				// Use non-atomic increment because all writes are performed
				// in the same event loop thread.
				if (++count == size) {
					runScan(client, eventLoop);
				}
			}

			public void onFailure(AerospikeException e) {
				console.error("Failed to put: " + e.getMessage());
				notifyComplete();
			}
		};

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, setName, i);
			Bin bin = new Bin(binName, i);

			client.put(eventLoop, listener, writePolicy, key, bin);
		}

		// Wait until scan finishes before closing cluster.  This is only necessary
		// when running the async scan example from the command line (which closes the
		// cluster after control is relinquished by this example).

		// The async scan will continue to run after cluster close if the scan was
		// initiated before cluster close.  The problem is cluster close shuts down
		// cluster tending immediately so any data partition migrations will not be
		// received by the client when performing the scan.
		waitTillComplete();
	}

	private void runScan(AerospikeClient client, EventLoop eventLoop) {
		int pageSize = 30;

		console.info("Scan max " + pageSize + " records.");

		ScanPolicy policy = new ScanPolicy();
		policy.maxRecords = pageSize;

		PartitionFilter filter = PartitionFilter.all();

		RecordSequenceListener listener = new RecordSequenceListener() {
			private int count = 0;

			@Override
			public void onRecord(Key key, Record record) throws AerospikeException {
				count++;
			}

			@Override
			public void onSuccess() {
				console.info("Records returned: " + count);
				notifyComplete();
			}

			@Override
			public void onFailure(AerospikeException e) {
				console.error("Scan failed: " + Util.getErrorMessage(e));
				notifyComplete();
			}
		};

		client.scanPartitions(eventLoop, listener, policy, filter, params.namespace, setName);
	}
}
