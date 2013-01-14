package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class PutGet extends Example {

	public PutGet(Console console) {
		super(console);
	}

	/**
	 * Write and read a bin value.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "putgetkey");
		Bin bin1 = new Bin(params.getBinName("bin1"), "value1");
		Bin bin2 = new Bin(params.getBinName("bin2"), "value2");

		console.info("Put: namespace=%s set=%s key=%s bin1=%s value1=%s bin2=%s value2=%s",
			key.namespace, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);
		
		client.put(params.writePolicy, key, bin1, bin2);

		console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

		Record record = client.get(params.policy, key);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
		}

		validateBin(key, bin1, record);
		validateBin(key, bin2, record);
	}
	
	private void validateBin(Key key, Bin bin, Record record) {
		Object received = record.getValue(bin.name);
		String expected = (String)bin.value;
		
		if (received.equals(expected)) {
			console.info("Bin matched: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Put/Get mismatch: Expected %s. Received %s.", expected, received);
		}
	}
}
