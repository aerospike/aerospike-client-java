/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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
	 * Perform operations on a set within a single bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Large set functions are not supported by the connected Aerospike server.");
			return;
		}

		Key key = new Key(params.namespace, params.set, "setkey");
		String binName = params.getBinName("setbin");
		
		// Delete record if it already exists.
		client.delete(params.writePolicy, key);
		
		// Initialize large set operator.
		com.aerospike.client.large.LargeSet set = client.getLargeSet(params.policy, key, binName, null);
						
		// Write values.
		set.add(Value.get("setvalue1"));
		set.add(Value.get("setvalue2"));
		//set.insert(Value.get("setvalue3"));
		
		// Verify large set was created with default configuration.
		Map<?,?> map = set.getConfig();
		
		for (Entry<?,?> entry : map.entrySet()) {
			console.info(entry.getKey().toString() + ',' + entry.getValue());
		}

		// Delete last value.
		// Comment out until delete supported on server.
		//set.delete(Value.get("setvalue3"));
		
		int size = set.size();
		
		if (size != 2) {
			throw new Exception("Size mismatch. Expected 2 Received " + size);
		}
		
		String received = (String)set.get(Value.get("setvalue2"));
		String expected = "setvalue2";
		
		if (received != null && received.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s value=%s", 
				key.namespace, key.setName, key.userKey, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
	}	
}
