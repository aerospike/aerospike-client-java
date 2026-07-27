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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;

public class Operate extends Example {

	/**
	 * Demonstrate multiple operations on a single record in one call.
	 */
	@Override
	public void runExample() throws Exception {
		// Write initial record.
		Key key = new Key(namespace(), set(), "opkey");
		Bin bin1 = new Bin("bin1", 7);
		Bin bin2 = new Bin("bin2", "string value");
		Bin bin3 = new Bin("bin3", 77.7);
		Bin bin4 = new Bin("bin4", "bin4val");

		console.info("Put: namespace=%s set=%s key=%s bin1=%s value1=%s bin2=%s value2=%s",
			key.namespace, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);
		client().put(writePolicy(), key, bin1, bin2, bin3, bin4);


		addWriteGet(key, bin1.name, bin2.name);
		touchReadMultipleBins(key, bin1.name, bin2.name, bin3.name);
	}

	private void addWriteGet(Key key, String bin1, String bin2) throws Exception {
		// Add integer, write new string and read record.
		Bin bin11 = new Bin(bin1, 4);
		Bin bin22 = new Bin(bin2, "new string");

		console.info("Add: " + bin11.value);
		console.info("Write: " + bin22.value);
		console.info("Read:");

		Record record = client().operate(writePolicy(), key, Operation.add(bin11), Operation.put(bin22), Operation.get());

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}
		console.info("Operate result: ns=%s set=%s key=%s bin1=%s bin2=%s generation=%d expiration=%d",
			key.namespace,
			key.setName,
			key.userKey,
			record.getInt(bin11.name),
			record.getValue(bin22.name),
			record.generation,
			record.expiration);
	}

	private void touchReadMultipleBins(
		Key key,
		String bin1,
		String bin2,
		String bin3
	) throws Exception {

		Record record = client().operate(writePolicy(), key,
			Operation.touch(), Operation.get(bin1), Operation.get(bin2), Operation.get(bin3));

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		console.info("Bin1: " + record.getInt(bin1));
		console.info("Bin2: " + record.getString(bin2));
		console.info("Bin3: " + record.getDouble(bin3));
	}
}
