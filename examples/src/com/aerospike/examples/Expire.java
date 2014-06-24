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
		noExpireExample(client,params);
	} // end runExample()
	
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
	} // end expireExample()
	
	/**
	 * Write and twice read a non-expiring tuple using the new "NoExpire" value (-1).
	 * This example is most effective when the Default Namespace Time To Live (TTL)
	 * is set to a small value, such as 5 seconds.  When we sleep beyond that
	 * time, we show that the NoExpire TTL flag actually works.
	 */
	private void noExpireExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "expirekey");
		Bin bin = new Bin(params.getBinName("expirebin"), "noexpirevalue");

		console.info("Put: namespace=%s set=%s key=%s bin=%s value=%s expiration=NoExpire",
				 key.namespace, key.setName, key.userKey, bin.name, bin.value);
		
		// Specify that record NEVER expires. 
		// The "Never Expire" value is -1, or 0xFFFFFFFF.
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = -1;
		client.put(writePolicy, key, bin);

		// Read the record, showing it is there.
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
		
		// Read this Record after the Default Expiration, showing it is still there.
		// We should have set the Namespace TTL at 5 sec.
		console.info("Sleeping for 10 seconds ...");
		Thread.sleep(10 * 1000);
		record = client.get(params.policy, key, bin.name);

		if (record == null) {
			console.error("Record expired and should NOT have.");
		}
		else {		
			console.info("Found Record (correctly) after default TTL.");
		}
	} // end noExpireExample()
} // end class Expire
