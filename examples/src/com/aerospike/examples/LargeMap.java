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

import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class LargeMap extends Example {

	public LargeMap(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a Large Map within a single bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Large Map functions are not supported by the connected Aerospike server.");
			return;
		}

		Key key = new Key(params.namespace, params.set, "setkey");
		String binName = params.getBinName("setbin");
		
		// Delete record if it already exists.
		client.delete(params.writePolicy, key);
		
		// Initialize Large Map operator.
		com.aerospike.client.large.LargeMap lmap = client.getLargeMap(params.writePolicy, key, binName, null);
						
		// Write values.
		lmap.put(Value.get("lmapName1"), Value.get("lmapValue1"));
		lmap.put(Value.get("lmapName2"), Value.get("lmapValue2"));
		lmap.put(Value.get("lmapName3"), Value.get("lmapValue3"));
		
		// Verify large set was created with default configuration.
		Map<?,?> map = lmap.getConfig();
		
		for (Entry<?,?> entry : map.entrySet()) {
			console.info(entry.getKey().toString() + ',' + entry.getValue());
		}

		// Remove last value.
		lmap.remove(Value.get("lmapName3"));
		
		int size = lmap.size();
		
		if (size != 2) {
			throw new Exception("Size mismatch. Expected 2 Received " + size);
		}
		
		Map<?,?> mapReceived = lmap.get(Value.get("lmapName2"));
		String expected = "lmapValue2";
		
		String stringReceived = (String) mapReceived.get("lmapName2");
		
		if (stringReceived != null && stringReceived.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s value=%s", 
				key.namespace, key.setName, key.userKey, stringReceived);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, stringReceived);
		}
	}	
}
