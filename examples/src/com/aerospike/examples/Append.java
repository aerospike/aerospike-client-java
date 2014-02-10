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
