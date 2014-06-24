/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class Serialize extends Example {

	public Serialize(Console console) {
		super(console);
	}

	/**
	 * Write complex objects using serializer.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {	
		testArray(client, params);
		testList(client, params);
		testComplex(client, params);
	}
	
	/**
	 * Write array of integers using serializer.
	 */
	public void testArray(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "serialarraykey");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		console.info("Initialize array");

		int[] array = new int[10000];

		for (int i = 0; i < 10000; i++) {
			array[i] = i * i;
		}

		Bin bin = new Bin(params.getBinName("serialbin"), array);

		// Do a test that pushes this complex object through the serializer
		console.info("Write array using serializer.");
		client.put(params.writePolicy, key, bin);

		console.info("Read array using serializer.");
		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		int[] received;

		try {
			received = (int[])record.getValue(bin.name);
		}
		catch (Exception e) {
			throw new Exception(String.format(
				"Failed to parse returned value: namespace=%s set=%s key=%s bin=%s", 
				key.namespace, key.setName, key.userKey, bin.name));
		}

		if (received.length != 10000) {
			throw new Exception(String.format(
				"Array length mismatch: Expected=%d Received=%d", 10000, received.length));
		}

		for (int i = 0; i < 10000; i++) {
			if (received[i] != i * i) {
				throw new Exception(String.format(
					"Mismatch: index=%d expected=%d received=%d", i, i*i, received[i])); 
			}
		}

		console.info("Read array successful.");
	}

	/**
	 * Write list object using serializer.
	 */
	public void testList(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "seriallistkey");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		console.info("Initialize list");
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");

		Bin bin = new Bin(params.getBinName("serialbin"), list);

		console.info("Write list using serializer.");
		client.put(params.writePolicy, key, bin);

		console.info("Read list using serializer.");
		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		List<?> received;

		try {
			received = (List<?>) record.getValue(bin.name);
		}
		catch (Exception e) {
			throw new Exception(String.format(
				"Failed to parse returned value: namespace=%s set=%s key=%s bin=%s", 
				key.namespace, key.setName, key.userKey, bin.name));
		}

		if (received.size() != 3) {
			throw new Exception(String.format(
				"Array length mismatch: Expected=%d Received=%d", 3, received.size()));
		}

		for (int i = 0; i < received.size(); i++) {
			String expected = "string" + (i + 1);
			if (! received.get(i).equals(expected)) {
				Object obj = received.get(i);
				throw new Exception(String.format(
					"Mismatch: index=%d expected=%s received=%s", i, expected, obj)); 
			}
		}

		console.info("Read list successful.");
	}
	
	/**
	 * Write complex object using serializer.
	 */
	public void testComplex(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "serialcomplexkey");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		console.info("Initialize complex object");
		
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

		Bin bin = new Bin(params.getBinName("complexbin"), list);

		console.info("Write complex object using serializer.");
		client.put(params.writePolicy, key, bin);

		console.info("Read complex object using serializer.");
		Record record = client.get(params.policy, key, bin.name);

		if (record == null) {
			throw new Exception(String.format(
				"Failed to get: namespace=%s set=%s key=%s",
				key.namespace, key.setName, key.userKey));
		}

		Object received;

		try {
			received = (List<?>) record.getValue(bin.name);
		}
		catch (Exception e) {
			throw new Exception(String.format(
				"Failed to parse returned value: namespace=%s set=%s key=%s bin=%s", 
				key.namespace, key.setName, key.userKey, bin.name));
		}

		if (received != null && received.equals(list)) {
			console.info("Data matched: namespace=%s set=%s key=%s bin=%s value=%s", 
				key.namespace, key.setName, key.userKey, bin.name, received);
		}
		else {
			console.error("Data mismatch");
			console.error("Expected " + list);
			console.error("Received " + received);
		}
		console.info("Read complex object successful.");
	}
}
