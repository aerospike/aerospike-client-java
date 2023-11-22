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

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;

public class OperateMap extends Example {

	public OperateMap(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a map bin.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) {
		runSimpleExample(client, params);
		runScoreExample(client, params);
		runListRangeExample(client, params);
		runNestedExample(client, params);
		runNestedMapCreateExample(client, params);
		runNestedListCreateExample(client, params);
	}

	/**
	 * Simple example of map operate functionality.
	 */
	public void runSimpleExample(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey");
		String binName = "mapbin";

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		Map<Value,Value> inputMap = new HashMap<Value,Value>();
		inputMap.put(Value.get(1), Value.get(55));
		inputMap.put(Value.get(2), Value.get(33));

		// Write values to empty map.
		Record record = client.operate(params.writePolicy, key,
				MapOperation.putItems(MapPolicy.Default, binName, inputMap)
				);

		console.info("Record: " + record);

		// Pop value from map and also return new size of map.
		record = client.operate(params.writePolicy, key,
				MapOperation.removeByKey(binName, Value.get(1), MapReturnType.VALUE),
				MapOperation.size(binName)
				);

		console.info("Record: " + record);

		// There should be one result for each map operation on the same map bin.
		// In this case, there are two map operations (pop and size), so there
		// should be two results.
		List<?> list = record.getList(binName);

		for (Object value : list) {
			console.info("Received: " + value);
		}
	}

	/**
	 * Map score example.
	 */
	public void runScoreExample(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey");
		String binName = "mapbin";

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		Map<Value,Value> inputMap = new HashMap<Value,Value>();
		inputMap.put(Value.get("Charlie"), Value.get(55));
		inputMap.put(Value.get("Jim"), Value.get(98));
		inputMap.put(Value.get("John"), Value.get(76));
		inputMap.put(Value.get("Harry"), Value.get(82));

		// Write values to empty map.
		Record record = client.operate(params.writePolicy, key,
				MapOperation.putItems(MapPolicy.Default, binName, inputMap)
				);

		console.info("Record: " + record);

		// Increment some user scores.
		record = client.operate(params.writePolicy, key,
				MapOperation.increment(MapPolicy.Default, binName, Value.get("John"), Value.get(5)),
				MapOperation.increment(MapPolicy.Default, binName, Value.get("Jim"), Value.get(-4))
				);

		console.info("Record: " + record);

		// Get top two scores.
		record = client.operate(params.writePolicy, key,
				MapOperation.getByRankRange(binName, -2, 2, MapReturnType.KEY_VALUE)
				);

		console.info("Record: " + record);

		// Print results.
		List<?> results = record.getList(binName);

		for (Object result : results) {
			console.info("Received: " + result);
		}
	}

	/**
	 * Value list range example.
	 */
	public void runListRangeExample(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey");
		String binName = "mapbin";

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		List<Value> l1 = new ArrayList<Value>();
		l1.add(Value.get(new GregorianCalendar(2018, 1, 1).getTimeInMillis()));
		l1.add(Value.get(1));

		List<Value> l2 = new ArrayList<Value>();
		l2.add(Value.get(new GregorianCalendar(2018, 1, 2).getTimeInMillis()));
		l2.add(Value.get(2));

		List<Value> l3 = new ArrayList<Value>();
		l3.add(Value.get(new GregorianCalendar(2018, 2, 1).getTimeInMillis()));
		l3.add(Value.get(3));

		List<Value> l4 = new ArrayList<Value>();
		l4.add(Value.get(new GregorianCalendar(2018, 2, 2).getTimeInMillis()));
		l4.add(Value.get(4));

		List<Value> l5 = new ArrayList<Value>();
		l5.add(Value.get(new GregorianCalendar(2018, 2, 5).getTimeInMillis()));
		l5.add(Value.get(5));

		Map<Value,Value> inputMap = new HashMap<Value,Value>();
		inputMap.put(Value.get("Charlie"), Value.get(l1));
		inputMap.put(Value.get("Jim"), Value.get(l2));
		inputMap.put(Value.get("John"), Value.get(l3));
		inputMap.put(Value.get("Harry"), Value.get(l4));
		inputMap.put(Value.get("Bill"), Value.get(l5));

		// Write values to empty map.
		Record record = client.operate(params.writePolicy, key,
				MapOperation.putItems(MapPolicy.Default, binName, inputMap)
				);

		console.info("Record: " + record);

		List<Value> end = new ArrayList<Value>();
		end.add(Value.get(new GregorianCalendar(2018, 2, 2).getTimeInMillis()));
		end.add(Value.getAsNull());

		// Delete values < end.
		record = client.operate(params.writePolicy, key,
				MapOperation.removeByValueRange(binName, null, Value.get(end), MapReturnType.COUNT)
				);

		console.info("Record: " + record);
	}

	/**
	 * Operate on a map of maps.
	 */
	public void runNestedExample(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey2");
		String binName = "mapbin";

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		Map<Value,Value> m1 = new HashMap<Value,Value>();
		m1.put(Value.get("key11"), Value.get(9));
		m1.put(Value.get("key12"), Value.get(4));

		Map<Value,Value> m2 = new HashMap<Value,Value>();
		m2.put(Value.get("key21"), Value.get(3));
		m2.put(Value.get("key22"), Value.get(5));

		Map<Value,Value> inputMap = new HashMap<Value,Value>();
		inputMap.put(Value.get("key1"), Value.get(m1));
		inputMap.put(Value.get("key2"), Value.get(m2));

		// Create maps.
		client.put(params.writePolicy, key, new Bin(binName, inputMap));

		// Set map value to 11 for map key "key21" inside of map key "key2"
		// and retrieve all maps.
		Record record = client.operate(params.writePolicy, key,
				MapOperation.put(MapPolicy.Default, binName, Value.get("key21"), Value.get(11), CTX.mapKey(Value.get("key2"))),
				Operation.get(binName)
				);

		record = client.get(params.policy, key);
		console.info("Record: " + record);
	}

	public void runNestedMapCreateExample(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey2");
		String binName = "mapbin";

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		Map<Value,Value> m1 = new HashMap<Value,Value>();
		m1.put(Value.get("key21"), Value.get(7));
		m1.put(Value.get("key22"), Value.get(6));

		Map<Value,Value> m2 = new HashMap<Value,Value>();
		m2.put(Value.get("a"), Value.get(3));
		m2.put(Value.get("c"), Value.get(5));

		Map<Value,Value> inputMap = new HashMap<Value,Value>();
		inputMap.put(Value.get("key1"), Value.get(m1));
		inputMap.put(Value.get("key2"), Value.get(m2));

		// Create maps.
		client.put(params.writePolicy, key, new Bin(binName, inputMap));

		// Create key ordered map at "key2" only if map does not exist.
		// Set map value to 4 for map key "key21" inside of map key "key2".
		CTX ctx = CTX.mapKey(Value.get("key2"));
		Record record = client.operate(params.writePolicy, key,
			MapOperation.create(binName, MapOrder.KEY_VALUE_ORDERED, ctx),
			MapOperation.put(MapPolicy.Default, binName, Value.get("b"), Value.get(4), ctx),
			Operation.get(binName)
			);

		record = client.get(params.policy, key);
		console.info("Record: " + record);
	}

	public void runNestedListCreateExample(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey3");
		String binName = "mapbin";

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		List<Value> l1 = new ArrayList<Value>();
		l1.add(Value.get(7));
		l1.add(Value.get(9));
		l1.add(Value.get(5));

		Map<Value,Value> inputMap = new HashMap<Value,Value>();
		inputMap.put(Value.get("key1"), Value.get(l1));

		// Create maps.
		client.put(params.writePolicy, key, new Bin(binName, inputMap));

		// Create ordered list at map's "key2" only if list does not exist.
		// Append 2,1 to ordered list.
		CTX ctx = CTX.mapKey(Value.get("key2"));
		Record record = client.operate(params.writePolicy, key,
			ListOperation.create(binName, ListOrder.ORDERED, false, ctx),
			ListOperation.append(binName, Value.get(2), ctx),
			ListOperation.append(binName, Value.get(1), ctx),
			Operation.get(binName)
			);

		record = client.get(params.policy, key);
		console.info("Record: " + record);
	}
}
