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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

public class TestListMap extends TestSync {
	@Test
	public void listStrings() {
		Key key = new Key(args.namespace, args.set, "listkey1");
		client.delete(null, key);

		ArrayList<String> list = new ArrayList<String>();
		list.add("string1");
		list.add("string2");
		list.add("string3");

		Bin bin = new Bin(args.getBinName("listbin1"), list);
		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		List<?> receivedList = (List<?>) record.getValue(bin.name);

		assertEquals(3, receivedList.size());
		assertEquals("string1", receivedList.get(0));
		assertEquals("string2", receivedList.get(1));
		assertEquals("string3", receivedList.get(2));
	}

	@Test
	public void listComplex() {
		Key key = new Key(args.namespace, args.set, "listkey2");
		client.delete(null, key);

		String geopoint =
			"{ \"type\": \"Point\", \"coordinates\": [0.00, 0.00] }";

		byte[] blob = new byte[] {3, 52, 125};
		ArrayList<Object> list = new ArrayList<Object>();
		list.add("string1");
		list.add(2);
		list.add(blob);
		list.add(Value.getAsGeoJSON(geopoint));

		Bin bin = new Bin(args.getBinName("listbin2"), list);
		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		List<?> receivedList = (List<?>) record.getValue(bin.name);

		assertEquals(4, receivedList.size());
		assertEquals("string1", receivedList.get(0));
		// Server convert numbers to long, so must expect long.
		assertEquals(2L, receivedList.get(1));
		assertArrayEquals(blob, (byte[])receivedList.get(2));
		assertEquals(Value.getAsGeoJSON(geopoint), receivedList.get(3));
	}

	@Test
	public void mapStrings() {
		Key key = new Key(args.namespace, args.set, "mapkey1");
		client.delete(null, key);

		HashMap<String,String> map = new HashMap<String,String>();
		map.put("key1", "string1");
		map.put("key2", "loooooooooooooooooooooooooongerstring2");
		map.put("key3", "string3");

		Bin bin = new Bin(args.getBinName("mapbin1"), map);
		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		Map<?,?> receivedMap = (Map<?,?>) record.getValue(bin.name);

		assertEquals(3, receivedMap.size());
		assertEquals("string1", receivedMap.get("key1"));
		assertEquals("loooooooooooooooooooooooooongerstring2", receivedMap.get("key2"));
		assertEquals("string3", receivedMap.get("key3"));
	}

	@Test
	public void mapComplex() {
		Key key = new Key(args.namespace, args.set, "mapkey2");
		client.delete(null, key);

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
		map.put("key5", true);
		map.put("key6", false);

		Bin bin = new Bin(args.getBinName("mapbin2"), map);
		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		Map<?,?> receivedMap = (Map<?,?>) record.getValue(bin.name);

		assertEquals(6, receivedMap.size());
		assertEquals("string1", receivedMap.get("key1"));
		// Server convert numbers to long, so must expect long.
		assertEquals(2L, receivedMap.get("key2"));
		assertArrayEquals(blob, (byte[])receivedMap.get("key3"));

		List<?> receivedInner = (List<?>)receivedMap.get("key4");
		assertEquals(4, receivedInner.size());
		assertEquals(100034L, receivedInner.get(0));
		assertEquals(12384955L, receivedInner.get(1));
		assertEquals(3L, receivedInner.get(2));
		assertEquals(512L, receivedInner.get(3));

		assertEquals(true, receivedMap.get("key5"));
		assertEquals(false, receivedMap.get("key6"));
	}

	@Test
	public void listMapCombined() {
		Key key = new Key(args.namespace, args.set, "listmapkey");
		client.delete(null, key);

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

		Bin bin = new Bin(args.getBinName("listmapbin"), list);
		client.put(null, key, bin);

		Record record = client.get(null, key, bin.name);
		List<?> received = (List<?>) record.getValue(bin.name);

		assertEquals(4, received.size());
		assertEquals("string1", received.get(0));
		// Server convert numbers to long, so must expect long.
		assertEquals(8L, received.get(1));

		List<?> receivedInner = (List<?>)received.get(2);
		assertEquals(2, receivedInner.size());
		assertEquals("string2", receivedInner.get(0));
		assertEquals(5L, receivedInner.get(1));

		Map<?,?> receivedMap = (Map<?,?>)received.get(3);
		assertEquals(4, receivedMap.size());
		assertEquals(1L, receivedMap.get("a"));
		assertEquals("b", receivedMap.get(2L));
		assertArrayEquals(blob, (byte[])receivedMap.get(3L));

		List<?> receivedInner2 = (List<?>)receivedMap.get("list");
		assertEquals(2, receivedInner2.size());
		assertEquals("string2", receivedInner2.get(0));
		assertEquals(5L, receivedInner2.get(1));
	}

	@Test
	public void sortedMapReplace() {
		Key key = new Key(args.namespace, args.set, "mapkey3");
		client.delete(null, key);

		List<Entry<Integer, String>> list = new ArrayList<Entry<Integer,String>>();
		list.add(new AbstractMap.SimpleEntry<Integer,String>(1, "s1"));
		list.add(new AbstractMap.SimpleEntry<Integer,String>(2, "s2"));
		list.add(new AbstractMap.SimpleEntry<Integer,String>(3, "s3"));

		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;

		Bin bin = new Bin("mapbin", list, MapOrder.KEY_VALUE_ORDERED);
		client.put(policy, key, bin);

		Record record = client.get(null, key, bin.name);
		//System.out.println(record);

		// It's unfortunate that the client returns a tree map here because
		// KEY_VALUE_ORDERED is sorted by key and then by value.  TreeMap
		// does not support this sorting behavior...
		//
		// TODO: Return List<Entry<?,?>> for KEY_VALUE_ORDERED maps.  This
		// would be a breaking change.
		TreeMap<?,?> receivedMap = (TreeMap<?,?>)record.getValue(bin.name);

		assertEquals(3, receivedMap.size());
		assertEquals("s1", receivedMap.get((long)1));
		assertEquals("s2", receivedMap.get((long)2));
		assertEquals("s3", receivedMap.get((long)3));
	}
}
