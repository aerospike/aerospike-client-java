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
	 * Write and twice read a bin value, demonstrating record expiration.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "expirekey");
		Bin bin = new Bin(params.getBinName("expirebin"), "expirevalue");

		console.info("Put: namespace=%s set=%s key=%s bin=%s value=%s expiration=2",
			key.namespace, key.setName, key.userKey, bin.name, bin.value);

		// Specify that record expires 2 seconds after it's written.
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 2;
		client.put(writePolicy, key, bin);

		// Read the record before it expires, showing it's there.
		console.info("Get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey);
		
		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		Object received = record.getValue(bin.name);
		String expected = bin.value.toString();
		
		if (received.equals(expected)) {
			console.info("Get successful: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			throw new Exception(String.format("Expire mismatch: Expected %s. Received %s.",
				expected, received));
		}

		// Read the record after it expires, showing it's gone.
		console.info("Sleeping for 3 seconds ...");
		Thread.sleep(3 * 1000);
		record = client.get(params.policy, key, bin.name);

		if (record == null) {
			console.info("Expiry successful. Record not found.");
		}
		else {		
			console.error("Found record when it should have expired.");
		}
	}
}
