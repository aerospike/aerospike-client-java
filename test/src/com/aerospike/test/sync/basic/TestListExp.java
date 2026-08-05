/*
 * Copyright 2012-2021 Aerospike, Inc.
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
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Packer;
import com.aerospike.test.sync.TestSync;

public class TestListExp extends TestSync {
	String binA = "A";
	String binB = "B";
	String binC = "C";

	Key keyA = new Key(args.namespace, args.set, binA);
	Key keyB = new Key(args.namespace, args.set, binB);

	Policy policy;

	@Before
	public void setUp() throws Exception {
		client.delete(null, keyA);
		client.delete(null, keyB);
		policy = new Policy();
	}

	@Test
	public void modifyWithContext() {
		List<Value> listSubA = new ArrayList<Value>();
		listSubA.add(Value.get("e"));
		listSubA.add(Value.get("d"));
		listSubA.add(Value.get("c"));
		listSubA.add(Value.get("b"));
		listSubA.add(Value.get("a"));

		List<Value> listA = new ArrayList<Value>();
		listA.add(Value.get("a"));
		listA.add(Value.get("b"));
		listA.add(Value.get("c"));
		listA.add(Value.get("d"));
		listA.add(Value.get(listSubA));

		List<Value> listB = new ArrayList<Value>();
		listB.add(Value.get("x"));
		listB.add(Value.get("y"));
		listB.add(Value.get("z"));

		client.operate(null, keyA,
			ListOperation.appendItems(ListPolicy.Default, binA, listA),
			ListOperation.appendItems(ListPolicy.Default, binB, listB),
			Operation.put(new Bin(binC, "M"))
			);

		CTX ctx = CTX.listIndex(4);
		Record record;
		List<?> result;

		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.size(
					// Temporarily append binB/binC to binA in expression.
					ListExp.appendItems(ListPolicy.Default, Exp.listBin(binB),
						ListExp.append(ListPolicy.Default, Exp.stringBin(binC), Exp.listBin(binA), ctx),
						ctx),
					ctx),
				Exp.val(9)));

		record = client.get(policy, keyA, binA);
		assertRecordFound(keyA, record);

		result = record.getList(binA);
		assertEquals(5, result.size());

		policy.filterExp = Exp.build(
			Exp.eq(
				ListExp.size(
					// Temporarily append local listB and local "M" string to binA in expression.
					ListExp.appendItems(ListPolicy.Default, Exp.val(listB),
						ListExp.append(ListPolicy.Default, Exp.val("M"), Exp.listBin(binA), ctx),
						ctx),
					ctx),
				Exp.val(9)));

		record = client.get(policy, keyA, binA);
		assertRecordFound(keyA, record);

		result = record.getList(binA);
		assertEquals(5, result.size());
	}

	@Test
	public void expReturnsList() {
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("a"));
		list.add(Value.get("b"));
		list.add(Value.get("c"));
		list.add(Value.get("d"));

		Expression exp = Exp.build(Exp.val(list));

		Record record = client.operate(null, keyA,
			ExpOperation.write(binC, exp, ExpWriteFlags.DEFAULT),
			Operation.get(binC),
			ExpOperation.read("var", exp, ExpReadFlags.DEFAULT)
			);

		//System.out.println(record);

		List<?> results = record.getList(binC);
		assertEquals(2, results.size());

		List<?> rlist = (List<?>)results.get(1);
		assertEquals(4, rlist.size());

		List<?> results2 = record.getList("var");
		assertEquals(4, results2.size());
	}

	@Test
	public void appendItemsUnsortedMapLiteral() {
		// CLIENT-5039: server 8.1.2.3+ (AER-6930) rejects expression map literals
		// that are not in canonical (key sorted) form.
		client.operate(null, keyA,
			ListOperation.appendItems(ListPolicy.Default, binA,
				Arrays.asList(Value.get(0), Value.get(1))));

		// LinkedHashMap is an unordered (non-SortedMap) HashMap with a
		// deterministic, deliberately unsorted iteration order.
		Map<String,Object> map = new LinkedHashMap<>();
		map.put("zz", 4L);
		map.put("aa", 1L);
		map.put("mm", 2L);
		map.put("cc", 3L);

		Expression exp = Exp.build(
			ListExp.size(
				ListExp.appendItems(ListPolicy.Default, Exp.val(Arrays.asList((Object)map)),
					Exp.listBin(binA))));

		Record record = client.operate(null, keyA,
			ExpOperation.read("result", exp, ExpReadFlags.DEFAULT));

		assertEquals(3, record.getLong("result"));
	}

	@Test
	public void appendItemsUnsortedIntKeyMapLiteral() {
		// Exact CLIENT-5039 ticket repro: integer keys in non-sorted order.
		client.operate(null, keyA,
			ListOperation.appendItems(ListPolicy.Default, binA,
				Arrays.asList(Value.get(0), Value.get(1))));

		Map<Object,Object> map = new LinkedHashMap<>();
		map.put(1402L, 1802L);
		map.put(2003L, 3946L);
		map.put(834L, 1374L);
		map.put(3117L, 1295L);

		Expression exp = Exp.build(
			ListExp.appendItems(ListPolicy.Default, Exp.val(Arrays.asList((Object)map)),
				Exp.listBin(binA)));

		Record record = client.operate(null, keyA,
			ExpOperation.read("result", exp, ExpReadFlags.DEFAULT));

		assertEquals(3, record.getList("result").size());
	}

	@Test
	public void nestedMapLiteralPacksCanonical() {
		Map<Object,Object> inner = new LinkedHashMap<>();
		inner.put("z", 26L);
		inner.put("a", 1L);

		Map<Object,Object> innerSorted = new LinkedHashMap<>();
		innerSorted.put("a", 1L);
		innerSorted.put("z", 26L);

		// Maps nested in list literals canonicalize at any depth.
		assertArrayEquals(
			Exp.build(Exp.val(Arrays.asList((Object)0L, inner))).getBytes(),
			Exp.build(Exp.val(Arrays.asList((Object)0L, innerSorted))).getBytes());

		// Maps nested as values of single-key maps canonicalize too.
		Map<Object,Object> outer = new LinkedHashMap<>();
		outer.put("k", inner);

		Map<Object,Object> outerSorted = new LinkedHashMap<>();
		outerSorted.put("k", innerSorted);

		assertArrayEquals(
			Exp.build(Exp.val(outer)).getBytes(),
			Exp.build(Exp.val(outerSorted)).getBytes());
	}

	@Test
	public void unsortedMapLiteralPacksCanonical() {
		Map<Object,Object> unsorted = new LinkedHashMap<>();
		unsorted.put("z", 26L);
		unsorted.put(5L, "five");
		unsorted.put("a", 1L);
		unsorted.put(-3L, "neg");

		Map<Object,Object> sorted = new LinkedHashMap<>();
		sorted.put(-3L, "neg");
		sorted.put(5L, "five");
		sorted.put("a", 1L);
		sorted.put("z", 26L);

		// Canonicalization is deterministic and adds no order header, so packed
		// bytes match a same-content map inserted in canonical order.
		assertArrayEquals(
			Exp.build(Exp.val(unsorted)).getBytes(),
			Exp.build(Exp.val(sorted)).getBytes());
	}

	@Test
	public void operationPathPreservesInsertionOrder() {
		Map<Object,Object> map = new LinkedHashMap<>();
		map.put("z", 26L);
		map.put("a", 1L);

		Map<Object,Object> reversed = new LinkedHashMap<>();
		reversed.put("a", 1L);
		reversed.put("z", 26L);

		// Non-expression packing (record writes, CDT operation arguments) must
		// keep map iteration order and stay byte-identical to previous releases.
		assertFalse(Arrays.equals(
			Packer.pack(map, MapOrder.UNORDERED),
			Packer.pack(reversed, MapOrder.UNORDERED)));
	}

	@Test
	public void appendItemsOperationUnsortedMap() {
		Map<Object,Object> map = new LinkedHashMap<>();
		map.put("z", 26L);
		map.put("a", 1L);
		map.put("m", 13L);

		client.operate(null, keyB,
			ListOperation.appendItems(ListPolicy.Default, binB,
				Arrays.asList(Value.get(0), Value.get(map))));

		Record record = client.get(null, keyB, binB);
		assertRecordFound(keyB, record);
		assertEquals(2, record.getList(binB).size());
	}
}
