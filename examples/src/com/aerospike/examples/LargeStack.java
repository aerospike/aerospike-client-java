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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

public class LargeStack extends Example {

	public LargeStack(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a stack within a single bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Large stack functions are not supported by the connected Aerospike server.");
			return;
		}

		Key key = new Key(params.namespace, params.set, "stackkey");
		String binName = params.getBinName("stackbin");
		
		// Delete record if it already exists.
		client.delete(params.writePolicy, key);
		
		// Initialize large stack operator.
		com.aerospike.client.large.LargeStack stack = client.getLargeStack(params.writePolicy, key, binName, null);
				
		// Write values.
		stack.push(Value.get("stackvalue1"));
		stack.push(Value.get("stackvalue2"));
		//stack.push(Value.get("stackvalue3"));
		
		// Verify large stack was created with default configuration.
		Map<?,?> map = stack.getConfig();
		
		for (Entry<?,?> entry : map.entrySet()) {
			console.info(entry.getKey().toString() + ',' + entry.getValue());
		}
			
		// Delete last value.
		// Comment out until trim supported on server.
		//stack.trim(1);
		
		int size = stack.size();
		
		if (size != 2) {
			throw new Exception("Size mismatch. Expected 2 Received " + size);
		}
		
		List<?> list = stack.peek(1);
		String received = (String)list.get(0);
		String expected = "stackvalue2";
		
		if (received != null && received.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s value=%s", 
				key.namespace, key.setName, key.userKey, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
		
		/*
		list = stack.peek(1, "myFilter", Value.get(1));
		received = (String)list.get(0);
		expected = "stackvalue2";
		
		if (received != null && received.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s value=%s", 
				key.namespace, key.setName, key.userKey, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}*/
	}	
}
