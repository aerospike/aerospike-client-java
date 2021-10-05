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
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionFilter;

public class ScanResume extends Example implements ScanCallback {

	private int recordCount;
	private int recordMax;

	public ScanResume(Console console) {
		super(console);
	}

	/**
	 * Terminate a scan and then resume scan later.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		String binName = "bin";
		String setName = "resume";

		writeRecords(client, params, setName, binName, 200);

		// Serialize node scans so scan callback atomics are not necessary.
		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = false;

		PartitionFilter filter = PartitionFilter.all();
		recordCount = 0;
		recordMax = 50;

		console.info("Start scan terminate");

		try {
			client.scanPartitions(policy, filter, params.namespace, setName, this);
		}
		catch (AerospikeException.ScanTerminated e) {
			console.info("Scan terminated as expected");
		}
		console.info("Records returned: " + recordCount);

		// PartitionFilter could be serialized at this point.
		// Resume scan now.
		recordCount = 0;
		recordMax = 0;

		console.info("Start scan resume");
		client.scanPartitions(policy, filter, params.namespace, setName, this);
		console.info("Records returned: " + recordCount);
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
		if (recordMax > 0 && recordCount >= recordMax) {
			// Terminate scan. The scan last digest will not be set and the current record
			// will be returned again if the scan resumes at a later time.
			throw new AerospikeException.ScanTerminated();
		}

		recordCount++;
	}
}
