/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.operation.BitOperation;
import com.aerospike.client.operation.BitPolicy;

public class OperateBit extends Example {

	public OperateBit(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a blob bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		runSimpleExample(client, params);
	}

	/**
	 * Simple example of bit functionality.
	 */
	public void runSimpleExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "bitkey");
		String binName = params.getBinName("bitbin");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		byte[] bytes = new byte[] {0b00000001, 0b00000010, 0b00000011, 0b00000100, 0b00000101};

		client.put(params.writePolicy, key, new Bin(binName, bytes));

		// Set last 3 bits of bitmap to true.
		Record record = client.operate(params.writePolicy, key,
				BitOperation.set(BitPolicy.Default, binName, -3, 3, new byte[] {(byte)0b11100000}),
				Operation.get(binName)
				);

		List<?> list = record.getList(binName);

		byte[] val = (byte[])list.get(1);

		for (byte b : val) {
			console.info(Byte.toString(b));
		}
	}
}
