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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;

public class Operate extends Example {

	public Operate(Console console) {
		super(console);
	}

	/**
	 * Demonstrate multiple operations on a single record in one call.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		// Write initial record.
		Key key = new Key(params.namespace, params.set, "opkey");
		Bin bin1 = new Bin("optintbin", 7);
		Bin bin2 = new Bin("optstringbin", "string value");		
		console.info("Put: namespace=%s set=%s key=%s bin1=%s value1=%s bin2=%s value2=%s",
			key.namespace, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);
		client.put(params.writePolicy, key, bin1, bin2);

		// Add integer, write new string and read record.
		Bin bin3 = new Bin(bin1.name, 4);
		Bin bin4 = new Bin(bin2.name, "new string");
		console.info("Add: " + bin3.value);
		console.info("Write: " + bin4.value);
		console.info("Read:");
		Record record = client.operate(params.writePolicy, key, Operation.add(bin3), Operation.put(bin4), Operation.get());

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		validateBin(key, record, bin3.name, 11, record.getValue(bin3.name));
		validateBin(key, record, bin4.name, bin4.value.toString(), record.getValue(bin4.name));	
	}
	
	private void validateBin(Key key, Record record, String binName, Object expected, Object received) {
		if (received != null && received.equals(expected)) {
			console.info("Bin matched: namespace=%s set=%s key=%s bin=%s value=%s generation=%s expiration=%s",
				key.namespace, key.setName, key.userKey, binName, received, record.generation, record.expiration);
		}
		else {
			console.error("Bin mismatch: Expected %s. Received %s.", expected, received);
		}
	}
}
