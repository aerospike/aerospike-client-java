/*
 * Copyright 2012-2016 Aerospike, Inc.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;

public class OperateMap extends Example {

	public OperateMap(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a list bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasCDTList) {
			console.info("CDT map functions are not supported by the connected Aerospike server.");
			return;
		}	
		runSimpleExample(client, params);
		runScoreExample(client, params);
	}

	/**
	 * Simple example of map operate functionality.
	 */
	public void runSimpleExample(AerospikeClient client, Parameters params) throws Exception {
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
	public void runScoreExample(AerospikeClient client, Parameters params) throws Exception {
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
		Map<?,?> results = record.getMap(binName);
		
		for (Entry<?,?> entry : results.entrySet()) {
			console.info("Received: " + entry);			
		}
	}
}
