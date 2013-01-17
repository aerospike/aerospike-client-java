package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class Serialize extends Example {

	public Serialize(Console console) {
		super(console);
	}

	/**
	 * Write complex object using serializer.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "serializetestserializetestserializetest");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		console.info("Initialize array");

		int[] array = new int[10000];

		for (int i = 0; i < 10000; i++) {
			array[i] = i * i;
		}

		Bin bin = new Bin(params.getBinName("serialbin"), array);

		// Do a test that pushes this complex object through the serializer
		console.info("Write array using serializer.");
		client.put(params.writePolicy, key, bin);

		console.info("Read array using serializer.");
		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		int[] received;

		try {
			received = (int[])record.getValue(bin.name);
		}
		catch (Exception e) {
			throw new Exception(String.format(
				"Failed to parse returned value: namespace=%s set=%s key=%s bin=%s", 
				key.namespace, key.setName, key.userKey, bin.name));
		}

		if (received.length != 10000) {
			throw new Exception(String.format(
				"Array length mismatch: Expected=%d Received=%d", 10000, received.length));
		}

		for (int i = 0; i < 10000; i++) {
			if (received[i] != i * i) {
				throw new Exception(String.format(
					"Mismatch: index=%d expected=%d received=%d", i, received[i], i*i)); 
			}
		}

		console.info("Read successful.");
	}
}
