package com.aerospike.examples;

import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class LargeSet extends Example {

	public LargeSet(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a list within a single bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Large set functions are not supported by the connected Aerospike server.");
			return;
		}

		Key key = new Key(params.namespace, params.set, "listkey");
		String binName = params.getBinName("listbin");
		
		// Delete record if it already exists.
		client.delete(params.writePolicy, key);
		
		// Initialize large set operator.
		com.aerospike.client.LargeSet set = client.getLargeSet(params.policy, key, binName);
		
		// Create large set in a single bin.
		set.create(null);
		
		// Verify large set was created with default configuration.
		Map<?,?> map = set.getConfig();
		
		for (Entry<?,?> entry : map.entrySet()) {
			console.info(entry.getKey().toString() + ',' + entry.getValue());
		}
		
		// Write values.
		set.insert(Value.get("listvalue1"));
		set.insert(Value.get("listvalue2"));
		//set.insert(Value.get("listvalue3"));
		
		// Delete last value.
		// Comment out until delete supported on server.
		//set.delete(Value.get("listvalue3"));
		
		int size = set.size();
		
		if (size != 2) {
			throw new Exception("Size mismatch. Expected 2 Received " + size);
		}
		
		String received = (String)set.get(Value.get("listvalue2"));
		String expected = "listvalue2";
		
		if (received != null && received.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s value=%s", 
				key.namespace, key.setName, key.userKey, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
	}	
}
