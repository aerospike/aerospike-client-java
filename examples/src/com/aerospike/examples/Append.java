/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class Append extends Example {

	/**
	 * Append string to an existing string.
	 */
	@Override
	public void runExample() throws Exception {
		Key key = new Key(namespace(), set(), "appendkey");
		String binName = "appendbin";

		client().delete(writePolicy(), key);

		Bin bin = new Bin(binName, "Hello");
		console.info("Initial append will create record.  Initial value is " + bin.value + '.');
		client().append(writePolicy(), key, bin);

		bin = new Bin(binName, " World");
		console.info("Append \"" + bin.value + "\" to existing record.");
		client().append(writePolicy(), key, bin);

		Record record = client().get(readPolicy(), key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}
		console.info("Append result: ns=%s set=%s key=%s bin=%s value=%s",
			key.namespace, key.setName, key.userKey, bin.name, record.getValue(bin.name));
	}
}
