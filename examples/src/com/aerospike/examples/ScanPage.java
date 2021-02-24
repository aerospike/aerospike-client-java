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
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionFilter;

public class ScanPage extends Example implements ScanCallback {

	private AtomicInteger recordCount;

	public ScanPage(Console console) {
		super(console);
	}

	/**
	 * Scan in pages.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		String binName = "bin";
		String setName = "page";

		writeRecords(client, params, setName, binName, 200);

		recordCount = new AtomicInteger();

		ScanPolicy policy = new ScanPolicy();
		policy.maxRecords = 100;

		PartitionFilter filter = PartitionFilter.all();

		// Scan 3 pages of records.
		for (int i = 0; i < 3 && ! filter.isDone(); i++) {
			recordCount.set(0);

			console.info("Scan page: " + i);
			client.scanPartitions(policy, filter, params.namespace, setName, this);
			console.info("Records returned: " + recordCount.get());
		}
	}

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String setName,
		String binName,
		int size
	) throws Exception {
		console.info("Write " + size + " records.");

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, setName, i);
			Bin bin = new Bin(binName, i);
			client.put(params.writePolicy, key, bin);
		}
	}

	@Override
	public void scanCallback(Key key, Record record) {
		// Scan callbacks must ensure thread safety when ScanAll() is used with
		// ScanPolicy concurrentNodes set to true (default).  In this case, parallel
		// node threads will be sending data to this callback.
		recordCount.incrementAndGet();

		/*
		synchronized (this) {
			int pid = Partition.getPartitionId(key.digest);
			console.info("PartId=" + pid + " Record=" + record);
		}
		*/
	}
}
