/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import java.util.HashMap;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.Bin;

public class UserDefinedFunction extends Example {

	/**
	 * Call user defined functions.
	 */
	@Override
	public void runExample() throws Exception {
		writeUsingUdf();
		writeIfGenerationNotChanged();
		writeIfNotExists();
		writeWithValidation();
		writeListMapUsingUdf();
		appendListUsingUdf();
		writeBlobUsingUdf();
	}

	private void writeUsingUdf() throws Exception {
		Key key = new Key(namespace(), set(), "udfkey1");
		Bin bin = new Bin("udfbin1", "string value");

		client().execute(writePolicy(), key, "record_example", "writeBin", Value.get(bin.name), bin.value);
		console.info("Wrote bin via UDF: namespace=%s set=%s key=%s bin=%s",
			key.namespace, key.setName, key.userKey, bin.name);
	}

	private void writeIfGenerationNotChanged() throws Exception {
		Key key = new Key(namespace(), set(), "udfkey2");
		Bin bin = new Bin("udfbin2", "string value");

		// Seed record.
		client().put(writePolicy(), key, bin);

		// Get record generation.
		long gen = (Long)client().execute(writePolicy(), key, "record_example", "getGeneration");

		// Write record if generation has not changed.
		client().execute(writePolicy(), key, "record_example", "writeIfGenerationNotChanged", Value.get(bin.name), bin.value, Value.get(gen));
		console.info("Record written.");
	}

	private void writeIfNotExists() throws Exception {
		Key key = new Key(namespace(), set(), "udfkey3");
		String binName = "udfbin3";

		// Write record only if not already exists. This should succeed.
		client().execute(writePolicy(), key, "record_example", "writeUnique", Value.get(binName), Value.get("first"));
		console.info("Record written if absent: namespace=%s set=%s key=%s bin=%s",
			key.namespace, key.setName, key.userKey, binName);

		// Write record second time. This should fail.
		console.info("Attempt second write.");
		client().execute(writePolicy(), key, "record_example", "writeUnique", Value.get(binName), Value.get("second"));
	}

	private void writeWithValidation() throws Exception {
		Key key = new Key(namespace(), set(), "udfkey4");
		String binName = "udfbin4";

		// Lua function writeWithValidation accepts number between 1 and 10.
		// Write record with valid value.
		console.info("Write with valid value.");
		client().execute(writePolicy(), key, "record_example", "writeWithValidation", Value.get(binName), Value.get(4));

		// Write record with invalid value.
		console.info("Write with invalid value.");
		boolean rejected = false;

		try {
			client().execute(writePolicy(), key, "record_example", "writeWithValidation", Value.get(binName), Value.get(11));
		}
		catch (Exception e) {
			rejected = true;
			console.info("Success. UDF resulted in exception as expected.");
		}

		if (! rejected) {
			throw new Exception("UDF should not have succeeded!");
		}
	}

	private void writeListMapUsingUdf() throws Exception {
		Key key = new Key(namespace(), set(), "udfkey5");

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

		String binName = "udfbin5";

		client().execute(writePolicy(), key, "record_example", "writeBin", Value.get(binName), Value.get(list));
		console.info("Stored list/map value via UDF: namespace=%s set=%s key=%s bin=%s",
			key.namespace, key.setName, key.userKey, binName);
	}

	private void appendListUsingUdf() throws Exception {
		Key key = new Key(namespace(), set(), "udfkey5");
		String binName = "udfbin5";
		String value = "appended value";

		client().execute(writePolicy(), key, "record_example", "appendListBin", Value.get(binName), Value.get(value));
		console.info("Appended list value via UDF: namespace=%s set=%s key=%s bin=%s value=%s",
			key.namespace, key.setName, key.userKey, binName, value);
	}

	private void writeBlobUsingUdf() throws Exception {
		Key key = new Key(namespace(), set(), "udfkey6");
		String binName = "udfbin6";

		// Create packed blob using standard java tools.
		byte[] blob;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (DataOutputStream dos = new DataOutputStream(baos)) {
				dos.writeInt(9845);
				dos.writeUTF("Hello world.");
			}
			blob = baos.toByteArray();
		}

		client().execute(writePolicy(), key, "record_example", "writeBin", Value.get(binName), Value.get(blob));
		console.info("Stored blob via UDF: namespace=%s set=%s key=%s bin=%s bytes=%s",
			key.namespace, key.setName, key.userKey, binName, blob.length);
	}
}
