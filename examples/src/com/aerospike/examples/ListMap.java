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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class ListMap extends Example {

	public ListMap(Console console) {
		super(console);
	}

	/**
	 * Write List and Map objects directly instead of relying on java serializer.
	 * This functionality is only supported in Aerospike 3.0 servers.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {	
		if (! params.hasUdf) {
			console.info("List/Map functions are not supported by the connected Aerospike server.");
			return;
		}
		testListStrings(client, params);
		testListComplex(client, params);
		testMapStrings(client, params);
		testMapComplex(client, params);
		testListMapCombined(client, params);
	}
	
	/**
	 * Write/Read ArrayList<String> directly instead of relying on java serializer.
	 */
	private void testListStrings(AerospikeClient client, Parameters params) throws Exception {
		console.info("Read/Write ArrayList<String>");
		Key key = new Key(params.namespace, params.set, "listkey1");
		client.delete(params.writePolicy, key);
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");

		Bin bin = Bin.asList(params.getBinName("listbin1"), list);
		client.put(params.writePolicy, key, bin);

		Record record = client.get(params.policy, key, bin.name);
		List<?> receivedList = (List<?>) record.getValue(bin.name);

		validateSize(3, receivedList.size());
		validate("string1", receivedList.get(0));
		validate("string2", receivedList.get(1));
		validate("string3", receivedList.get(2));

		console.info("Read/Write ArrayList<String> successful.");
	}
	
	/**
	 * Write/Read ArrayList<Object> directly instead of relying on java serializer.
	 */
	private void testListComplex(AerospikeClient client, Parameters params) throws Exception {
		console.info("Read/Write ArrayList<Object>");
		Key key = new Key(params.namespace, params.set, "listkey2");
		client.delete(params.writePolicy, key);

		byte[] blob = new byte[] {3, 52, 125};		
		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(2);
		list.add(blob);

		Bin bin = Bin.asList(params.getBinName("listbin2"), list);
		client.put(params.writePolicy, key, bin);

		Record record = client.get(params.policy, key, bin.name);
		List<?> receivedList = (List<?>) record.getValue(bin.name);

		validateSize(3, receivedList.size());
		validate("string1", receivedList.get(0));
		// Server convert numbers to long, so must expect long.
		validate(2L, receivedList.get(1)); 
		validate(blob, (byte[])receivedList.get(2));
		
		console.info("Read/Write ArrayList<Object> successful.");
	}
	
	/**
	 * Write/Read HashMap<String,String> directly instead of relying on java serializer.
	 */
	private void testMapStrings(AerospikeClient client, Parameters params) throws Exception {
		console.info("Read/Write HashMap<String,String>");
		Key key = new Key(params.namespace, params.set, "mapkey1");
		client.delete(params.writePolicy, key);

		HashMap<String,String> map = new HashMap<String,String>();
		map.put("key1", "string1");
		map.put("key2", "string2");
		map.put("key3", "string3");

		Bin bin = Bin.asMap(params.getBinName("mapbin1"), map);
		client.put(params.writePolicy, key, bin);

		Record record = client.get(params.policy, key, bin.name);
		Map<?,?> receivedMap = (Map<?,?>) record.getValue(bin.name);
		
		validateSize(3, receivedMap.size());
		validate("string1", receivedMap.get("key1"));
		validate("string2", receivedMap.get("key2"));
		validate("string3", receivedMap.get("key3"));

		console.info("Read/Write HashMap<String,String> successful");
	}

	/**
	 * Write/Read HashMap<Object,Object> directly instead of relying on java serializer.
	 */
	private void testMapComplex(AerospikeClient client, Parameters params) throws Exception {
		console.info("Read/Write HashMap<Object,Object>");
		Key key = new Key(params.namespace, params.set, "mapkey2");
		client.delete(params.writePolicy, key);

		byte[] blob = new byte[] {3, 52, 125};		
		List<Integer> list = new ArrayList<Integer>();
		list.add(100034);
		list.add(12384955);
		list.add(3);
		list.add(512);
		
		HashMap<Object,Object> map = new HashMap<Object,Object>();
		map.put("key1", "string1");
		map.put("key2", 2);
		map.put("key3", blob);
		map.put("key4", list);  // map.put("key4", Value.getAsList(list)) works too

		Bin bin = Bin.asMap(params.getBinName("mapbin2"), map);
		client.put(params.writePolicy, key, bin);

		Record record = client.get(params.policy, key, bin.name);
		Map<?,?> receivedMap = (Map<?,?>) record.getValue(bin.name);
		
		validateSize(4, receivedMap.size());
		validate("string1", receivedMap.get("key1"));
		// Server convert numbers to long, so must expect long.
		validate(2L, receivedMap.get("key2"));
		validate(blob, (byte[])receivedMap.get("key3"));
				
		List<?> receivedInner = (List<?>)receivedMap.get("key4");
		validateSize(4, receivedInner.size());
		validate(100034L, receivedInner.get(0));
		validate(12384955L, receivedInner.get(1));
		validate(3L, receivedInner.get(2));
		validate(512L, receivedInner.get(3));

		console.info("Read/Write HashMap<Object,Object> successful");
	}

	/**
	 * Write/Read List/HashMap combination directly instead of relying on java serializer.
	 */
	private void testListMapCombined(AerospikeClient client, Parameters params) throws Exception {
		console.info("Read/Write List/HashMap");
		Key key = new Key(params.namespace, params.set, "listmapkey");
		client.delete(params.writePolicy, key);

		byte[] blob = new byte[] {3, 52, 125};		
		ArrayList<Object> inner = new ArrayList<Object>();
		inner.add("string2");
		inner.add(5);
		
		HashMap<Object,Object> innerMap = new HashMap<Object,Object>();
		innerMap.put("a", 1);
		innerMap.put(2, "b");
		innerMap.put(3, blob);
		innerMap.put("list", inner);
		
		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(8);
		list.add(inner);
		list.add(innerMap);

		Bin bin = Bin.asList(params.getBinName("listmapbin"), list);
		client.put(params.writePolicy, key, bin);

		Record record = client.get(params.policy, key, bin.name);
		List<?> received = (List<?>) record.getValue(bin.name);
		
		validateSize(4, received.size());
		validate("string1", received.get(0));
		// Server convert numbers to long, so must expect long.
		validate(8L, received.get(1));
		
		List<?> receivedInner = (List<?>)received.get(2);
		validateSize(2, receivedInner.size());
		validate("string2", receivedInner.get(0));
		validate(5L, receivedInner.get(1));
		
		Map<?,?> receivedMap = (Map<?,?>)received.get(3);
		validateSize(4, receivedMap.size());
		validate(1L, receivedMap.get("a"));
		validate("b", receivedMap.get(2L));
		validate(blob, (byte[])receivedMap.get(3L));

		List<?> receivedInner2 = (List<?>)receivedMap.get("list");
		validateSize(2, receivedInner2.size());
		validate("string2", receivedInner2.get(0));
		validate(5L, receivedInner2.get(1));

		console.info("Read/Write List/HashMap successful");
	}

	private static void validateSize(int expected, int received) throws Exception {		
		if (received != expected) {
			throw new Exception(String.format(
				"Size mismatch: expected=%s received=%s", expected, received)); 
		}
	}

	private static void validate(Object expected, Object received) throws Exception {
		if (! received.equals(expected)) {
			throw new Exception(String.format(
				"Mismatch: expected=%s received=%s", expected, received)); 
		}
	}

	private static void validate(byte[] expected, byte[] received) throws Exception {		
		if (! Arrays.equals(expected, received)) {
			throw new Exception(String.format(
				"Mismatch: expected=%s received=%s", Arrays.toString(expected), Arrays.toString(received))); 
		}
	}
}
