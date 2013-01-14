package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;

public class Add extends Example {

	public Add(Console console) {
		super(console);
	}

	/**
	 * Add integer values.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "addkey");
		Bin bin = new Bin(params.getBinName("addbin"), 10);

		// Delete record if it already exists.
		client.delete(params.policy, key);

		// Perform some adds and check results.
		console.info("Initial add will create record.  Initial value is " + bin.value + '.');
		client.add(params.writePolicy, key, bin);

		bin.value = 5;
		console.info("Add " + bin.value + " to existing record.");
		client.add(params.writePolicy, key, bin);

		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		// The value received from the server is an unsigned byte stream.
		// Convert to an integer before comparing with expected.
		int received = (Integer)record.getValue(bin.name);
		int expected = 15;

		if (received == expected) {
			console.info("Add successful: ns=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Add mismatch: Expected %d. Received %d.", expected, received);
		}

		// Demonstrate add and get combined.
		bin.value = 30;
		console.info("Add " + bin.value + " to existing record.");		
		record = client.operate(params.writePolicy, key, Operation.add(bin), Operation.get(bin.name));

		expected = 45;
		received = (Integer)record.getValue(bin.name);

		if (received == expected) {
			console.info("Add successful: ns=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Add mismatch: Expected %d. Received %d.", expected, received);
		}
	}
}
