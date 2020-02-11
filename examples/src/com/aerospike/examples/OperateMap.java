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
package com.aerospike.examples;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
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
	public void runExample(AerospikeClient client, Parameters params) {
		if (! params.hasCDTMap) {
			console.info("CDT map functions are not supported by the connected Aerospike server.");
			return;
		}
		runSimpleExample(client, params);
		runScoreExample(client, params);
		runListRangeExample(client, params);
		runNestedExample(client, params);
	}

	/**
	 * Simple example of map operate functionality.
	 */
	public void runSimpleExample(AerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey");
		String binName = params.getBinName("mapbin");

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
	public void runScoreExample(AerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey");
		String binName = params.getBinName("mapbin");

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
				MapOperation.decrement(MapPolicy.Default, binName, Value.get("Jim"), Value.get(4))
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
	public void runListRangeExample(AerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey");
		String binName = params.getBinName("mapbin");

		// Delete record if it already exists.
		client.delete(params.writePolicy, key);

		List<Value> l1 = new ArrayList<Value>();
		l1.add(Value.get(new GregorianCalendar(2018, 1, 1).getTime()));
		l1.add(Value.get(1));

		List<Value> l2 = new ArrayList<Value>();
		l2.add(Value.get(new GregorianCalendar(2018, 1, 2).getTime()));
		l2.add(Value.get(2));

		List<Value> l3 = new ArrayList<Value>();
		l3.add(Value.get(new GregorianCalendar(2018, 2, 1).getTime()));
		l3.add(Value.get(3));

		List<Value> l4 = new ArrayList<Value>();
		l4.add(Value.get(new GregorianCalendar(2018, 2, 2).getTime()));
		l4.add(Value.get(4));

		List<Value> l5 = new ArrayList<Value>();
		l5.add(Value.get(new GregorianCalendar(2018, 2, 5).getTime()));
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
		end.add(Value.get(new GregorianCalendar(2018, 2, 2).getTime()));
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
	public void runNestedExample(AerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "mapkey2");
		String binName = params.getBinName("mapbin");

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
}
