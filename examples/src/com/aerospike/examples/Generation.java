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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;

public class Generation extends Example {

	public Generation(Console console) {
		super(console);
	}

	/**
	 * Exercise record generation functionality.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "genkey");
		String binName = params.getBinName("genbin");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		// Set some values for the same record.
		Bin bin = new Bin(binName, "genvalue1");
		console.info("Put: namespace=%s set=%s key=%s bin=%s value=%s",
			key.namespace, key.setName, key.userKey, bin.name, bin.value);
		
		client.put(params.writePolicy, key, bin);

		bin = new Bin(binName, "genvalue2");
		console.info("Put: namespace=%s set=%s key=%s bin=%s value=%s",
			key.namespace, key.setName, key.userKey, bin.name, bin.value);

		client.put(params.writePolicy, key, bin);

		// Retrieve record and its generation count.
		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		Object received = record.getValue(bin.name);
		String expected = bin.value.toString();
		
		if (received.equals(expected)) {
			console.info("Get successful: namespace=%s set=%s key=%s bin=%s value=%s generation=%d", 
				key.namespace, key.setName, key.userKey, bin.name, received, record.generation);
		}
		else {
			throw new Exception(String.format("Get mismatch: Expected %s. Received %s.",
				expected, received));
		}

		// Set record and fail if it's not the expected generation.
		bin = new Bin(binName, "genvalue3");
		console.info("Put: namespace=%s set=%s key=%s bin=%s value=%s expected generation=%d",
			key.namespace, key.setName, key.userKey, bin.name, bin.value, record.generation);

		WritePolicy writePolicy = new WritePolicy();
		writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		writePolicy.generation = record.generation;
		client.put(writePolicy, key, bin);

		// Set record with invalid generation and check results .
		bin = new Bin(binName, "genvalue4");
		writePolicy.generation = 9999;
		console.info("Put: namespace=%s set=%s key=%s bin=%s value=%s expected generation=%d",
			key.namespace, key.setName, key.userKey, bin.name, bin.value, writePolicy.generation);

		try {
			client.put(writePolicy, key, bin);
			throw new Exception("Should have received generation error instead of success.");				
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
				console.info("Success: Generation error returned as expected.");				
			}
			else {
				throw new Exception(String.format(
					"Unexpected set return code: namespace=%s set=%s key=%s bin=%s value=%s code=%s", 
					key.namespace, key.setName, key.userKey, bin.name, bin.value, ae.getResultCode()));				
			}
		}

		// Verify results.
		record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		received = record.getValue(bin.name);
		expected = "genvalue3";
		
		if (received.equals(expected)) {
			console.info("Get successful: namespace=%s set=%s key=%s bin=%s value=%s generation=%d", 
				key.namespace, key.setName, key.userKey, bin.name, received, record.generation);
		}
		else {
			throw new Exception(String.format("Get mismatch: Expected %s. Received %s.",
				expected, received));
		}		
	}
}
