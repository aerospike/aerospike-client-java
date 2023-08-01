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

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;

public class ScanParallel extends Example implements ScanCallback {

	private AtomicInteger recordCount;

	public ScanParallel(Console console) {
		super(console);
	}

	/**
	 * Scan all nodes in parallel and read all records in a set.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		console.info("Scan parallel: namespace=" + params.namespace + " set=" + params.set);
		recordCount = new AtomicInteger();
		long begin = System.currentTimeMillis();
		ScanPolicy policy = new ScanPolicy();
		client.scanAll(policy, params.namespace, params.set, this);

		long end = System.currentTimeMillis();
		double seconds =  (double)(end - begin) / 1000.0;
		int count = recordCount.get();
		console.info("Total records returned: " + count);
		console.info("Elapsed time: " + seconds + " seconds");
		double performance = Math.round((double)count / seconds);
		console.info("Records/second: " + performance);
	}

	@Override
	public void scanCallback(Key key, Record record) {
		// Scan callbacks must ensure thread safety when ScanAll() is used with
		// ScanPolicy concurrentNodes set to true (default).  In this case, parallel
		// node threads will be sending data to this callback.
		int count = recordCount.incrementAndGet();

		if ((count % 10000) == 0) {
			console.info("Records " + count);
		}
	}
}
