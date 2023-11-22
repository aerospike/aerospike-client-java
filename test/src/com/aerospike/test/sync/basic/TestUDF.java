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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.BatchUDF;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;

public class TestUDF extends TestSync {
	@BeforeClass
	public static void register() {
		if (args.useProxyClient) {
			System.out.println("Skip TestUDF.register");
			return;
		}
		RegisterTask task = client.register(null, TestUDF.class.getClassLoader(), "udf/record_example.lua", "record_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	@Test
	public void writeUsingUdf() {
		Key key = new Key(args.namespace, args.set, "udfkey1");
		Bin bin = new Bin("udfbin1", "string value");

		client.execute(null, key, "record_example", "writeBin", Value.get(bin.name), bin.value);

		Record record = client.get(null, key, bin.name);
		assertBinEqual(key, record, bin);
	}

	@Test
	public void writeIfGenerationNotChanged() {
		Key key = new Key(args.namespace, args.set, "udfkey2");
		Bin bin = new Bin("udfbin2", "string value");

		// Seed record.
		client.put(null, key, bin);

		// Get record generation.
		long gen = (Long)client.execute(null, key, "record_example", "getGeneration");

		// Write record if generation has not changed.
		client.execute(null, key, "record_example", "writeIfGenerationNotChanged", Value.get(bin.name), bin.value, Value.get(gen));
	}

	@Test
	public void writeIfNotExists() {
		Key key = new Key(args.namespace, args.set, "udfkey3");
		String binName = "udfbin3";

		// Delete record if it already exists.
		client.delete(null, key);

		// Write record only if not already exists. This should succeed.
		client.execute(null, key, "record_example", "writeUnique", Value.get(binName), Value.get("first"));

		// Verify record written.
		Record record = client.get(null, key, binName);
		assertBinEqual(key, record, binName, "first");

		// Write record second time. This should fail.
		client.execute(null, key, "record_example", "writeUnique", Value.get(binName), Value.get("second"));

		// Verify record not written.
		record = client.get(null, key, binName);
		assertBinEqual(key, record, binName, "first");
	}

	@Test
	public void writeWithValidation() {
		Key key = new Key(args.namespace, args.set, "udfkey4");
		String binName = "udfbin4";

		// Lua function writeWithValidation accepts number between 1 and 10.
		// Write record with valid value.
		client.execute(null, key, "record_example", "writeWithValidation", Value.get(binName), Value.get(4));

		// Write record with invalid value.
		try {
			client.execute(null, key, "record_example", "writeWithValidation", Value.get(binName), Value.get(11));
			fail("UDF should not have succeeded!");
		}
		catch (Exception e) {
		}
	}

	@Test
	public void writeListMapUsingUdf() {
		Key key = new Key(args.namespace, args.set, "udfkey5");

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

		client.execute(null, key, "record_example", "writeBin", Value.get(binName), Value.get(list));

		Object received = client.execute(null, key, "record_example", "readBin", Value.get(binName));
		assertNotNull(received);
		assertEquals(list, received);
	}

	@Test
	public void appendListUsingUdf() {
		Key key = new Key(args.namespace, args.set, "udfkey5");
		String binName = "udfbin5";
		String value = "appended value";

		client.execute(null, key, "record_example", "appendListBin", Value.get(binName), Value.get(value));

		Record record = client.get(null, key, binName);
		assertRecordFound(key, record);

		Object received = record.getValue(binName);

		if (received != null && received instanceof List<?>) {
			List<?> list = (List<?>)received;

			if (list.size() == 5) {
				Object obj = list.get(4);

				if (obj.equals(value)) {
					return;
				}
			}
		}
		fail("UDF data mismatch" + System.lineSeparator() +
			 "Expected: " + value + System.lineSeparator() +
			 "Received: " + received);
	}

	@Test
	public void writeBlobUsingUdf() {
		Key key = new Key(args.namespace, args.set, "udfkey6");
		String binName = "udfbin6";

		// Create packed blob using standard java tools.
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			dos.writeInt(9845);
			dos.writeUTF("Hello world.");
		}
		catch (Exception e) {
			fail("DataOutputStream error: " + e.getMessage());
		}
		byte[] blob = baos.toByteArray();

		client.execute(null, key, "record_example", "writeBin", Value.get(binName), Value.get(blob));
		byte[] received = (byte[])client.execute(null, key, "record_example", "readBin", Value.get(binName));
		assertArrayEquals(blob, received);
	}

	@Test
	public void batchUDF() {
		Key[] keys = new Key[] {
			new Key(args.namespace, args.set, 20000),
			new Key(args.namespace, args.set, 20001)
		};

		client.delete(null, null, keys);

		BatchResults br = client.execute(null, null, keys, "record_example", "writeBin", Value.get("B5"), Value.get("value5"));
		assertTrue(br.status);

		Record[] records = client.get(null, keys, "B5");
		assertEquals(2, records.length);

		for (Record r : records) {
			assertNotNull(r);
			assertEquals("value5", r.getString("B5"));
		}
	}

	@Test
	public void batchUDFError() {
		Key[] keys = new Key[] {
			new Key(args.namespace, args.set, 20002),
			new Key(args.namespace, args.set, 20003)
		};

		client.delete(null, null, keys);

		BatchResults br = client.execute(null, null, keys, "record_example", "writeWithValidation", Value.get("B5"), Value.get(999));
		assertFalse(br.status);

		for (BatchRecord r : br.records) {
			assertNotNull(r);
			assertEquals(ResultCode.UDF_BAD_RESPONSE, r.resultCode);

			String msg = r.record.getUDFError();
			//System.out.println(msg);
			assertNotNull(msg);
		}
	}

	@Test
	public void batchUDFComplex() {
		String bin = "B5";

		Value[] a1 = new Value[] {Value.get(bin), Value.get("value1")};
		Value[] a2 = new Value[] {Value.get(bin), Value.get(5)};
		Value[] a3 = new Value[] {Value.get(bin), Value.get(999)};

		BatchUDF b1 = new BatchUDF(new Key(args.namespace, args.set, 20004), "record_example", "writeBin", a1);
		BatchUDF b2 = new BatchUDF(new Key(args.namespace, args.set, 20005), "record_example", "writeWithValidation", a2);
		BatchUDF b3 = new BatchUDF(new Key(args.namespace, args.set, 20005), "record_example", "writeWithValidation", a3);

		List<BatchRecord> records = new ArrayList<BatchRecord>();
		records.add(b1);
		records.add(b2);
		records.add(b3);

		boolean status = client.operate(null, records);

		assertFalse(status); // b3 results in an error.
		assertBinEqual(b1.key, b1.record, bin, 0);
		assertBinEqual(b2.key, b2.record, bin, 0);
		assertEquals(ResultCode.UDF_BAD_RESPONSE, b3.resultCode);

		BatchRead b4 = new BatchRead(new Key(args.namespace, args.set, 20004), true);
		BatchRead b5 = new BatchRead(new Key(args.namespace, args.set, 20005), true);

		records.clear();
		records.add(b4);
		records.add(b5);

		status = client.operate(null, records);

		assertTrue(status);
		assertBinEqual(b4.key, b4.record, bin, "value1");
		assertBinEqual(b5.key, b5.record, bin, 5);
	}
}
