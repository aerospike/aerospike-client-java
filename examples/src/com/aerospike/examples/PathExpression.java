/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.CdtOperation;
import com.aerospike.client.cdt.SelectFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.LoopVarPart;

public class PathExpression extends Example {

	public PathExpression(Console console) {
		super(console);
	}

	/**
	 * Demonstrate path expression enhancements: CTX.mapKeys, CTX.andFilter,
	 * Exp.inList, Exp.mapKeys, and Exp.mapValues.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		runMapKeysSelect(client, params);
		runMapKeysWithAndFilter(client, params);
		runInListExpression(client, params);
		runMapKeysExpression(client, params);
		runMapValuesExpression(client, params);
	}

	/**
	 * Use CTX.mapKeys to select a subset of map entries by key list
	 * via CdtOperation.selectByPath.
	 */
	private void runMapKeysSelect(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "pathexp1");
		String binName = "mapbin";

		client.delete(params.writePolicy, key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);
		map.put("Harry", 82);

		client.put(params.writePolicy, key, new Bin(binName, map));

		console.info("Map: " + map);

		// Select only "Charlie" and "John" values using CTX.mapKeys.
		CTX ctx = CTX.mapKeys(Arrays.asList("Charlie", "John"));
		Record record = client.operate(params.writePolicy, key,
			CdtOperation.selectByPath(binName, SelectFlags.VALUE, ctx)
		);

		console.info("selectByPath mapKeys [Charlie, John]: " + record.getList(binName));
	}

	/**
	 * Use CTX.mapKeys combined with CTX.andFilter to select map entries
	 * by key list and then further filter by value.
	 */
	private void runMapKeysWithAndFilter(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "pathexp2");
		String binName = "mapbin";

		client.delete(params.writePolicy, key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);
		map.put("Harry", 82);

		client.put(params.writePolicy, key, new Bin(binName, map));

		console.info("Map: " + map);

		// Select keys "Charlie", "Jim", "John", then keep only entries with value > 70.
		CTX keyCtx = CTX.mapKeys(Arrays.asList("Charlie", "Jim", "John"));
		CTX filter = CTX.andFilter(
			Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(70))
		);

		Record record = client.operate(params.writePolicy, key,
			CdtOperation.selectByPath(binName, SelectFlags.MAP_KEY_VALUE, keyCtx, filter)
		);

		console.info("selectByPath mapKeys [Charlie, Jim, John] AND value > 70: " + record.getValue(binName));
	}

	/**
	 * Use Exp.inList to check if a bin value is contained in a list.
	 */
	private void runInListExpression(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "pathexp3");

		client.delete(params.writePolicy, key);

		client.put(params.writePolicy, key,
			new Bin("color", "blue"),
			new Bin("size", 10)
		);

		console.info("Record: color=blue, size=10");

		// Check if "color" bin value is in the list ["red", "blue", "green"].
		Expression exp = Exp.build(
			Exp.inList(
				Exp.stringBin("color"),
				Exp.val(Arrays.asList("red", "blue", "green"))
			)
		);

		Record record = client.operate(null, key,
			ExpOperation.read("inList", exp, ExpReadFlags.DEFAULT)
		);

		console.info("inList [red, blue, green] contains 'blue': " + record.getBoolean("inList"));

		// Negative case: "blue" is not in ["red", "yellow", "green"].
		Expression expNot = Exp.build(
			Exp.inList(
				Exp.stringBin("color"),
				Exp.val(Arrays.asList("red", "yellow", "green"))
			)
		);

		Record recordNot = client.operate(null, key,
			ExpOperation.read("notInList", expNot, ExpReadFlags.DEFAULT)
		);

		console.info("inList [red, yellow, green] contains 'blue': " + recordNot.getBoolean("notInList"));
	}

	/**
	 * Use Exp.mapKeys to extract all keys from a map bin.
	 */
	private void runMapKeysExpression(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "pathexp4");
		String binName = "mapbin";

		client.delete(params.writePolicy, key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);

		client.put(params.writePolicy, key, new Bin(binName, map));

		console.info("Map: " + map);

		// Extract all keys from the map.
		Expression exp = Exp.build(
			Exp.mapKeys(Exp.mapBin(binName))
		);

		Record record = client.operate(null, key,
			ExpOperation.read("keys", exp, ExpReadFlags.DEFAULT)
		);

		List<?> keys = record.getList("keys");
		console.info("Exp.mapKeys: " + keys);
	}

	/**
	 * Use Exp.mapValues to extract all values from a map bin.
	 */
	private void runMapValuesExpression(IAerospikeClient client, Parameters params) {
		Key key = new Key(params.namespace, params.set, "pathexp5");
		String binName = "mapbin";

		client.delete(params.writePolicy, key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);

		client.put(params.writePolicy, key, new Bin(binName, map));

		console.info("Map: " + map);

		// Extract all values from the map.
		Expression exp = Exp.build(
			Exp.mapValues(Exp.mapBin(binName))
		);

		Record record = client.operate(null, key,
			ExpOperation.read("values", exp, ExpReadFlags.DEFAULT)
		);

		List<?> values = record.getList("values");
		console.info("Exp.mapValues: " + values);
	}
}
