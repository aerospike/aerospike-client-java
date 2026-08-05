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

public class PutGet extends Example {

	/**
	 * Write and read a bin value.
	 */
	@Override
	public void runExample() throws Exception {
		runMultiBinTest();
		runGetHeaderTest();
	}

	/**
	 * Execute put and get on a server configured as multi-bin.  This is the server default.
	 */
	private void runMultiBinTest() throws Exception {
		Key key = new Key(namespace(), set(), "putgetkey");
		Bin bin1 = new Bin("bin1", "value1");
		Bin bin2 = new Bin("bin2", "value2");

		console.info("Put: namespace=%s set=%s key=%s bin1=%s value1=%s bin2=%s value2=%s",
			key.namespace, key.setName, key.userKey, bin1.name, bin1.value, bin2.name, bin2.value);

		client().put(writePolicy(), key, bin1, bin2);

		console.info("Get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);

		Record record = client().get(readPolicy(), key);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
		}

		console.info("Received: namespace=%s set=%s key=%s bin1=%s bin2=%s generation=%d expiration=%d",
			key.namespace,
			key.setName,
			key.userKey,
			record.getValue(bin1.name),
			record.getValue(bin2.name),
			record.generation,
			record.expiration);
	}

	/**
	 * Read record header data.
	 */
	private void runGetHeaderTest() throws Exception {
		Key key = new Key(namespace(), set(), "putgetkey");

		console.info("Get record header: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey);
		Record record = client().getHeader(readPolicy(), key);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s", key.namespace, key.setName, key.userKey));
		}

		console.info("Received: generation=%d expiration=%d", record.generation, record.expiration);
	}
}
