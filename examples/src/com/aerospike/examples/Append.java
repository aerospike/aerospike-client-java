package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class Append extends Example {

	public Append(Console console) {
		super(console);
	}

	/**
	 * Append string to an existing string.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "appendkey");
		String binName = params.getBinName("appendbin");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		Bin bin = new Bin(binName, "Hello");
		console.info("Initial append will create record.  Initial value is " + bin.value + '.');
		client.append(params.writePolicy, key, bin);

		bin = new Bin(binName, " World");
		console.info("Append \"" + bin.value + "\" to existing record.");
		client.append(params.writePolicy, key, bin);

		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		Object received = record.getValue(bin.name);
		String expected = "Hello World";

		if (received.equals(expected)) {
			console.info("Append successful: ns=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Append mismatch: Expected %s. Received %s.", expected, received);
		}
	}
}
