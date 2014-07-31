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
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class LargeList extends Example {

	public LargeList(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a Large List within a single bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Large List functions are not supported by the connected Aerospike server.");
			return;
		}

		Key key = new Key(params.namespace, params.set, "setkey");
		String binName = params.getBinName("ListBin");
		
		// Delete record if it already exists.
		client.delete(params.writePolicy, key);
		
		// Initialize large set operator.
		com.aerospike.client.large.LargeList llist = client.getLargeList(params.policy, key, binName, null);
		String orig1 = "llistValue1";
		String orig2 = "llistValue2";
		String orig3 = "llistValue3";
						
		// Write values.
		llist.add(Value.get(orig1));
		llist.add(Value.get(orig2));
		llist.add(Value.get(orig3));
		
		// Verify large list was created with default configuration.
		Map<?,?> map = llist.getConfig();
		
		for (Entry<?,?> entry : map.entrySet()) {
			console.info(entry.getKey().toString() + ',' + entry.getValue());
		}
		
		// Perform a Range Query -- look for "llistValue2" to "llistValue3"
		List<?> rangeList = llist.range(Value.get(orig2), Value.get(orig3));
		if ( rangeList.size() != 2 ) {
			throw new Exception("Range Size mismatch. Expected 2 Received " + rangeList.size());
		}
		String v2 = (String) rangeList.get(0);
		String v3 = (String) rangeList.get(1);
		
		if ( v2.equals(orig2) && v3.equals(orig3) ) {
			console.info("Range Query matched: v2=%s v3=%s", orig2, orig3);
		} else {
			throw new Exception("Range Content mismatch. Expected (%s:%s) Received (%s:%s) " 
					+ orig2 + orig3 + v2 + v3);
		}

		// Remove last value.
		llist.remove(Value.get(orig3));
		
		int size = llist.size();
		
		if (size != 2) {
			throw new Exception("Size mismatch. Expected 2 Received " + size);
		}
		
		List<?> listReceived = llist.find(Value.get(orig2));
		String expected = orig2;
		String stringReceived = (String) listReceived.get(0);
		
		if (stringReceived != null && stringReceived.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s value=%s", 
				key.namespace, key.setName, key.userKey, stringReceived);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, stringReceived);
		}
	}	
}
