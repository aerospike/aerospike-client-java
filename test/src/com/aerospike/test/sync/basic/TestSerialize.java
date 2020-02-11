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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.test.sync.TestSync;

public class TestSerialize extends TestSync {
	private static final String binName = args.getBinName("serialbin");

	@Test
	public void serializeArray() {
		Key key = new Key(args.namespace, args.set, "serialarraykey");

		// Delete record if it already exists.
		client.delete(null, key);

		int[] array = new int[10000];

		for (int i = 0; i < 10000; i++) {
			array[i] = i * i;
		}

		Bin bin = new Bin(binName, array);

		// Do a test that pushes this complex object through the serializer
		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		assertRecordFound(key, record);

		int[] received = null;

		try {
			received = (int[])record.getValue(bin.name);
		}
		catch (Exception e) {
			fail("Failed to parse returned value: namespace=" + key.namespace + " set=" + key.setName +
				 " key=" + key.userKey + " bin=" + bin.name);
		}

		assertNotNull(received);
		assertEquals(10000, received.length);

		for (int i = 0; i < 10000; i++) {
			if (received[i] != i * i) {
				fail("Mismatch: index=" + i + " expected=" + (i*i) + " received=" + received[i]);
			}
		}
	}

	@Test
	public void serializeList() {
		Key key = new Key(args.namespace, args.set, "seriallistkey");

		// Delete record if it already exists.
		client.delete(null, key);

		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");

		Bin bin = new Bin(binName, (Object)list);

		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		assertRecordFound(key, record);

		List<?> received = null;

		try {
			received = (List<?>) record.getValue(bin.name);
		}
		catch (Exception e) {
			fail("Failed to parse returned value: namespace=" + key.namespace + " set=" + key.setName +
				 " key=" + key.userKey + " bin=" + bin.name);
		}

		assertNotNull(received);
		assertEquals(3, received.size());
		int max = received.size();

		for (int i = 0; i < max; i++) {
			String expected = "string" + (i + 1);
			if (! received.get(i).equals(expected)) {
				Object obj = received.get(i);
				fail("Mismatch: index=" + i + " expected=" + expected + " received=" + obj);
			}
		}
	}

	@Test
	public void serializeComplex() {
		Key key = new Key(args.namespace, args.set, "serialcomplexkey");

		// Delete record if it already exists.
		client.delete(null, key);

		ArrayList<Object> inner = new ArrayList<Object>();
		inner.add("string2");
		inner.add(8);

		HashMap<Object,Object> innerMap = new HashMap<Object,Object>();
		innerMap.put("a", 1);
		innerMap.put(2, "b");
		innerMap.put("list", inner);

		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(4);
		list.add(inner);
		list.add(innerMap);

		Bin bin = new Bin(args.getBinName("complexbin"), new Value.BlobValue(list));

		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		assertRecordFound(key, record);

		Object received = null;

		try {
			received = (List<?>) record.getValue(bin.name);
		}
		catch (Exception e) {
			fail("Failed to parse returned value: namespace=" + key.namespace + " set=" + key.setName +
				 " key=" + key.userKey + " bin=" + bin.name);
		}

		if (received == null || ! received.equals(list)) {
			fail("Data mismatch" + System.lineSeparator() +
				"Expected " + list + System.lineSeparator() +
				"Received " + received);
		}
	}
}
