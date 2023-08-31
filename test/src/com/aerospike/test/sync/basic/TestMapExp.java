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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.Policy;
import com.aerospike.test.sync.TestSync;

public class TestMapExp extends TestSync {
	@Test
	public void sortedMapEquality() {
		TreeMap<String,String> map = new TreeMap<>();
		map.put("key1", "e");
		map.put("key2", "d");
		map.put("key3", "c");
		map.put("key4", "b");
		map.put("key5", "a");

		Key key = new Key(args.namespace, args.set, "sme");
		Bin bin = new Bin("m", map);

		client.put(null, key, bin);

		Policy p = new Policy();
		p.filterExp = Exp.build(Exp.eq(Exp.mapBin(bin.name), Exp.val(map)));

		Record rec = client.get(p, key);
		assertRecordFound(key, rec);

		Map<?,?> m = rec.getMap(bin.name);

		if (!(m instanceof TreeMap)) {
			fail("Map not instance of TreeMap");
		}
	}

	@Test
	public void invertedMapExp() {
		HashMap<String,Integer> map = new HashMap<>();
		map.put("a", 1);
		map.put("b", 2);
		map.put("c", 2);
		map.put("d", 3);

		Key key = new Key(args.namespace, args.set, "ime");
		Bin bin = new Bin("m", map);

		client.put(null, key, bin);

		// Use INVERTED to return map with entries removed where value != 2.
		Expression e = Exp.build(MapExp.removeByValue(MapReturnType.INVERTED, Exp.val(2), Exp.mapBin(bin.name)));

		Record rec = client.operate(null, key, ExpOperation.read(bin.name, e, ExpReadFlags.DEFAULT));
		assertRecordFound(key, rec);

		Map<?,?> m = rec.getMap(bin.name);
		assertEquals(2L, m.size());
		assertEquals(2L, m.get("b"));
		assertEquals(2L, m.get("c"));
	}
}
