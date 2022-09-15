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

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;

public class ScanParallel extends Example implements ScanCallback {

	private AtomicInteger recordCount;
	private String setName;

	public ScanParallel(Console console) {
		super(console);
	}

	/**
	 * Scan all nodes in parallel and read all records in a set.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		recordCount = new AtomicInteger();
		setName = params.set;
		console.info("Scan parallel: namespace=" + params.namespace + " set=" + setName);
		client.scanAll(null, params.namespace, setName, this);
		int count = recordCount.get();
		console.info("Total records returned: " + count);
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

		if (!setName.equals(key.setName)) {
			console.error("Set mismatch. Expected " + setName + " Received " + key.setName);
		}
	}
}
