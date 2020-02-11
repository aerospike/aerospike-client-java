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
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;

public class Expire extends Example {

	public Expire(Console console) {
		super(console);
	}

	/**
	 * Demonstrate various record expiration settings.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		expireExample(client, params);
	}

	/**
	 * Write and twice read an expiration record.
	 */
	private void expireExample(AerospikeClient client, Parameters params) throws Exception {
		Key key  = new Key(params.namespace, params.set, "expirekey ");
		Bin bin  = new Bin(params.getBinName("expirebin"), "expirevalue");

		console.info("Put: namespace=%s set=%s key=%s bin=%s value=%s expiration=2",
			key.namespace, key.setName, key.userKey, bin.name, bin.value);

		// Specify that record expires 2 seconds after it's written.
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 2;
		client.put(writePolicy, key, bin);

		// Read the record before it expires, showing it is there.
		console.info("Get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey);

		Record record = client.get(params.policy, key, bin.name);
		if (record == null) {
			throw new Exception(String.format(
				"Failed to get record: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		Object received = record.getValue(bin.name);
		String expected = bin.value.toString();
		if (received.equals(expected)) {
			console.info("Get record successful: namespace=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			throw new Exception(String.format("Expire record mismatch: Expected %s. Received %s.",
				expected, received));
		}

		// Read the record after it expires, showing it's gone.
		console.info("Sleeping for 3 seconds ...");
		Thread.sleep(3 * 1000);
		record = client.get(params.policy, key, bin.name);
		if (record == null) {
			console.info("Expiry of record successful. Record not found.");
		}
		else {
			console.error("Found record when it should have expired.");
		}
	}
}
