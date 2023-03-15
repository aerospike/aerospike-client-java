/*
 * Copyright 2012-2022 Aerospike, Inc.
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.task.RegisterTask;

public class UserDefinedFunction extends Example {

	public UserDefinedFunction(Console console) {
		super(console);
	}

	/**
	 * Register user defined function and call it.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		// Register is not supported in the proxy client. To run this example with the proxy client,
		// first run example with native client (which supports register) and then run proxy client.
		if (! params.useProxyClient) {
			register(client, params);
		}

		writeUsingUdf(client, params);
		writeIfGenerationNotChanged(client, params);
		writeIfNotExists(client, params);
		writeWithValidation(client, params);
		writeListMapUsingUdf(client, params);
		appendListUsingUdf(client, params);
		writeBlobUsingUdf(client, params);
	}

	private void register(IAerospikeClient client, Parameters params) throws Exception {
		String filename = "record_example.lua";
		console.info("Register: " + filename);
		RegisterTask task = client.register(params.policy, "udf/record_example.lua", filename, Language.LUA);
		task.waitTillComplete();
	}

	private void writeUsingUdf(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey1");
		Bin bin = new Bin(params.getBinName("udfbin1"), "string value");

		client.execute(params.writePolicy, key, "record_example", "writeBin", Value.get(bin.name), bin.value);

		Record record = client.get(params.policy, key, bin.name);
		String expected = bin.value.toString();
		String received = record.getString(bin.name);

		if (received != null && received.equals(expected)) {
			console.info("Data matched: namespace=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
	}

	private void writeIfGenerationNotChanged(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey2");
		Bin bin = new Bin(params.getBinName("udfbin2"), "string value");

		// Seed record.
		client.put(params.writePolicy, key, bin);

		// Get record generation.
		long gen = (Long)client.execute(params.writePolicy, key, "record_example", "getGeneration");

		// Write record if generation has not changed.
		client.execute(params.writePolicy, key, "record_example", "writeIfGenerationNotChanged", Value.get(bin.name), bin.value, Value.get(gen));
		console.info("Record written.");
	}

	private void writeIfNotExists(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey3");
		String binName = "udfbin3";

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		// Write record only if not already exists. This should succeed.
		client.execute(params.writePolicy, key, "record_example", "writeUnique", Value.get(binName), Value.get("first"));

		// Verify record written.
		Record record = client.get(params.policy, key, binName);
		String expected = "first";
		String received = record.getString(binName);

		if (received != null && received.equals(expected)) {
			console.info("Record written: namespace=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, binName, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}

		// Write record second time. This should fail.
		console.info("Attempt second write.");
		client.execute(params.writePolicy, key, "record_example", "writeUnique", Value.get(binName), Value.get("second"));

		// Verify record not written.
		record = client.get(params.policy, key, binName);
		received = record.getString(binName);

		if (received != null && received.equals(expected)) {
			console.info("Success. Record remained unchanged: namespace=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, binName, received);
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, received);
		}
	}

	private void writeWithValidation(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey4");
		String binName = "udfbin4";

		// Lua function writeWithValidation accepts number between 1 and 10.
		// Write record with valid value.
		console.info("Write with valid value.");
		client.execute(params.writePolicy, key, "record_example", "writeWithValidation", Value.get(binName), Value.get(4));

		// Write record with invalid value.
		console.info("Write with invalid value.");

		try {
			client.execute(params.writePolicy, key, "record_example", "writeWithValidation", Value.get(binName), Value.get(11));
			console.error("UDF should not have succeeded!");
		}
		catch (Exception e) {
			console.info("Success. UDF resulted in exception as expected.");
		}
	}

	private void writeListMapUsingUdf(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey5");

		ArrayList<Object> inner = new ArrayList<Object>();
		inner.add("string2");
		inner.add(8L);

		HashMap<Object,Object> innerMap = new HashMap<Object,Object>();
		innerMap.put("a", 1L);
		innerMap.put(2L, "b");
		innerMap.put("list", inner);

		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(4L);
		list.add(inner);
		list.add(innerMap);

		String binName = params.getBinName("udfbin5");

		client.execute(params.writePolicy, key, "record_example", "writeBin", Value.get(binName), Value.get(list));

		Object received = client.execute(params.writePolicy, key, "record_example", "readBin", Value.get(binName));

		if (received != null && received.equals(list)) {
			console.info("UDF data matched: namespace=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, binName, received);
		}
		else {
			console.error("UDF data mismatch");
			console.error("Expected " + list);
			console.error("Received " + received);
		}
	}

	private void appendListUsingUdf(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey5");
		String binName = params.getBinName("udfbin5");
		String value = "appended value";

		client.execute(params.writePolicy, key, "record_example", "appendListBin", Value.get(binName), Value.get(value));

		Record record = client.get(params.policy, key, binName);

		if (record != null) {
			Object received = record.getValue(binName);

			if (received != null && received instanceof List<?>) {
				List<?> list = (List<?>)received;

				if (list.size() == 5) {
					Object obj = list.get(4);

					if (obj.equals(value)) {
						console.info("UDF data matched: namespace=%s set=%s key=%s bin=%s value=%s",
								key.namespace, key.setName, key.userKey, binName, received);
						return;
					}
				}
			}
			console.error("UDF data mismatch");
			console.error("Expected: " + value);
			console.error("Received: " + received);
		}
		else {
			console.error("Failed to find record: " + key.userKey);
		}
	}

	private void writeBlobUsingUdf(IAerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "udfkey6");
		String binName = params.getBinName("udfbin6");

		// Create packed blob using standard java tools.
		byte[] blob;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (DataOutputStream dos = new DataOutputStream(baos)) {
				dos.writeInt(9845);
				dos.writeUTF("Hello world.");
			}
			blob = baos.toByteArray();
		}

		client.execute(params.writePolicy, key, "record_example", "writeBin", Value.get(binName), Value.get(blob));
		byte[] received = (byte[])client.execute(params.writePolicy, key, "record_example", "readBin", Value.get(binName));

		if (Arrays.equals(blob, received)) {
			console.info("Blob data matched: namespace=%s set=%s key=%s bin=%s value=%s",
					key.namespace, key.setName, key.userKey, binName, Arrays.toString(received));
		}
		else {
			throw new Exception(String.format(
				"Mismatch: expected=%s received=%s", Arrays.toString(blob), Arrays.toString(received)));
		}
	}
}
