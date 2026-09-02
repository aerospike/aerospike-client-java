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
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.CdtOperation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.SelectFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.LoopVarPart;

public class PathExpression extends Example {

	/**
	 * Demonstrate path expression enhancements: CTX.mapKeysIn (string and {@link Value} varargs),
	 * CTX.andFilter, Exp.inList, Exp.mapKeysIn, and Exp.mapValues.
	 */
	@Override
	public void runExample() throws Exception {
		runMapKeysSelect();
		runMapKeysInValueMixedSelect();
		runMapKeysWithAndFilter();
		runInListExpression();
		runMapKeysExpression();
		runMapValuesExpression();
	}

	/**
	 * Use CTX.mapKeysIn to select a subset of map entries by key list
	 * via CdtOperation.selectByPath.
	 */
	private void runMapKeysSelect() {
		Key key = new Key(namespace(), set(), "pathexp1");
		String binName = "mapbin";

		client().delete(writePolicy(), key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);
		map.put("Harry", 82);

		client().put(writePolicy(), key, new Bin(binName, map));

		console.info("Map: " + map);

		// Select only "Charlie" and "John" values using CTX.mapKeysIn.
		CTX ctx = CTX.mapKeysIn("Charlie", "John");
		Record record = client().operate(writePolicy(), key,
			CdtOperation.selectByPath(binName, SelectFlags.VALUE, ctx)
		);

		console.info("selectByPath mapKeysIn [Charlie, John]: " + record.getList(binName));
	}

	/**
	 * Use {@link CTX#mapKeysIn(Value...)} to select map entries when keys use more than one
	 * CDT type (here: string, integer, and blob) in a single path context. Requires server 8.1.2+.
	 */
	private void runMapKeysInValueMixedSelect() {
		Key key = new Key(namespace(), set(), "pathexp6");
		String binName = "mapbin";

		client().delete(writePolicy(), key);

		byte[] regionKey = new byte[] { 'u', 's', '-', 'e', 'a', 's', 't' };
		Map<Value, Value> map = new HashMap<>();
		map.put(Value.get("sku"), Value.get("standard"));
		map.put(Value.get(1001L), Value.get("express"));
		map.put(Value.get(regionKey), Value.get("regional-offer"));

		client().operate(writePolicy(), key,
			MapOperation.putItems(MapPolicy.Default, binName, map));

		console.info("Mixed-key map stored (string sku, long 1001, blob region key).");

		CTX ctx = CTX.mapKeysIn(Value.get("sku"), Value.get(1001L), Value.get(regionKey));
		Record record = client().operate(writePolicy(), key,
			CdtOperation.selectByPath(binName, SelectFlags.VALUE, ctx));

		console.info("selectByPath mapKeysIn(Value...) [sku, 1001, region]: " + record.getList(binName));
	}

	/**
	 * Use CTX.mapKeysIn combined with CTX.andFilter to select map entries
	 * by key list and then further filter by value.
	 */
	private void runMapKeysWithAndFilter() {
		Key key = new Key(namespace(), set(), "pathexp2");
		String binName = "mapbin";

		client().delete(writePolicy(), key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);
		map.put("Harry", 82);

		client().put(writePolicy(), key, new Bin(binName, map));

		console.info("Map: " + map);

		// Select keys "Charlie", "Jim", "John", then keep only entries with value > 70.
		CTX keyCtx = CTX.mapKeysIn("Charlie", "Jim", "John");
		CTX filter = CTX.andFilter(
			Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(70))
		);

		Record record = client().operate(writePolicy(), key,
			CdtOperation.selectByPath(binName, SelectFlags.MAP_KEY_VALUE, keyCtx, filter)
		);

		console.info("selectByPath mapKeysIn [Charlie, Jim, John] AND value > 70: " + record.getValue(binName));
	}

	/**
	 * Use Exp.inList to check if a bin value is contained in a list.
	 */
	private void runInListExpression() {
		Key key = new Key(namespace(), set(), "pathexp3");

		client().delete(writePolicy(), key);

		client().put(writePolicy(), key,
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

		Record record = client().operate(null, key,
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

		Record recordNot = client().operate(null, key,
			ExpOperation.read("notInList", expNot, ExpReadFlags.DEFAULT)
		);

		console.info("inList [red, yellow, green] contains 'blue': " + recordNot.getBoolean("notInList"));
	}

	/**
	 * Use Exp.mapKeysIn to extract all keys from a map bin.
	 */
	private void runMapKeysExpression() {
		Key key = new Key(namespace(), set(), "pathexp4");
		String binName = "mapbin";

		client().delete(writePolicy(), key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);

		client().put(writePolicy(), key, new Bin(binName, map));

		console.info("Map: " + map);

		// Extract all keys from the map.
		Expression exp = Exp.build(
			Exp.mapKeysIn(Exp.mapBin(binName))
		);

		Record record = client().operate(null, key,
			ExpOperation.read("keys", exp, ExpReadFlags.DEFAULT)
		);

		List<?> keys = record.getList("keys");
		console.info("Exp.mapKeysIn: " + keys);
	}

	/**
	 * Use Exp.mapValues to extract all values from a map bin.
	 */
	private void runMapValuesExpression() {
		Key key = new Key(namespace(), set(), "pathexp5");
		String binName = "mapbin";

		client().delete(writePolicy(), key);

		Map<String, Integer> map = new HashMap<>();
		map.put("Charlie", 55);
		map.put("Jim", 98);
		map.put("John", 76);

		client().put(writePolicy(), key, new Bin(binName, map));

		console.info("Map: " + map);

		// Extract all values from the map.
		Expression exp = Exp.build(
			Exp.mapValuesIn(Exp.mapBin(binName))
		);

		Record record = client().operate(null, key,
			ExpOperation.read("values", exp, ExpReadFlags.DEFAULT)
		);

		List<?> values = record.getList("values");
		console.info("Exp.mapValues: " + values);
	}
}
