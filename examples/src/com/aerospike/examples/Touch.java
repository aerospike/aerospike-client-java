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
import com.aerospike.client.policy.WritePolicy;

public class Touch extends Example {

	public Touch(Console console) {
		super(console);
	}

	/**
	 * Demonstrate touch command.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "touchkey");
		Bin bin = new Bin(params.getBinName("touchbin"), "touchvalue");

		console.info("Create record with 2 second expiration.");
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 2;
		client.put(writePolicy, key, bin);
		
		console.info("Touch same record with 5 second expiration.");
		writePolicy.expiration = 5;
		Record record = client.operate(writePolicy, key, Operation.touch(), Operation.getHeader());

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, null));
		}

		if (record.expiration == 0) {
			throw new Exception(String.format(
				"Failed to get record expiration: namespace=%s set=%s key=%s", 
				key.namespace, key.setName, key.userKey));
		}
		
		console.info("Sleep 3 seconds.");
		Thread.sleep(3000);

		record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		console.info("Success. Record still exists.");
		console.info("Sleep 4 seconds.");
		Thread.sleep(4000);

		record = client.get(params.policy, key, bin.name);

		if (record == null) {
			console.info("Success. Record expired as expected.");
		}
		else {		
			console.error("Found record when it should have expired.");
		}
	}
}
