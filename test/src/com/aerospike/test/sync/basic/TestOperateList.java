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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListSortFlags;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

public class TestOperateList extends TestSync {
	private static final String binName = "oplistbin";

	@Test
	public void operateList1() {
		Key key = new Key(args.namespace, args.set, "oplkey1");

		client.delete(null, key);

		// Calling append() multiple times performs poorly because the server makes
		// a copy of the list for each call, but we still need to test it.
		// Using appendItems() should be used instead for best performance.
		Record record = client.operate(null, key,
				ListOperation.append(binName, Value.get(55)),
				ListOperation.append(binName, Value.get(77)),
				ListOperation.pop(binName, -1),
				ListOperation.size(binName)
				);

		assertRecordFound(key, record);

		List<?> list = record.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(1, size);

		size = (Long)list.get(1);
		assertEquals(2, size);

		long val = (Long)list.get(2);
		assertEquals(77, val);

		size = (Long)list.get(3);
		assertEquals(1, size);
	}

	@Test
	public void operateList2() {
		Key key = new Key(args.namespace, args.set, "oplkey2");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(12));
		itemList.add(Value.get(-8734));
		itemList.add(Value.get("my string"));

		Record record = client.operate(null, key,
				ListOperation.appendItems(binName, itemList),
				Operation.put(new Bin("otherbin", "hello"))
				);

		assertRecordFound(key, record);

		record = client.operate(null, key,
				ListOperation.insert(binName, -1, Value.get(8)),
				Operation.append(new Bin("otherbin", Value.get("goodbye"))),
				Operation.get("otherbin"),
				ListOperation.getRange(binName, 0, 4),
				ListOperation.getRange(binName, 3)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		String val = record.getString("otherbin");
		assertEquals("hellogoodbye", val);

		List<?> list = record.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(4, size);

		List<?> rangeList = (List<?>)list.get(1);
		long lval = (Long)rangeList.get(0);
		assertEquals(12, lval);

		lval = (Long)rangeList.get(1);
		assertEquals(-8734, lval);

		lval = (Long)rangeList.get(2);
		assertEquals(8, lval);

		val = (String)rangeList.get(3);
		assertEquals("my string", val);

		rangeList = (List<?>)list.get(2);
		val = (String)rangeList.get(0);
		assertEquals("my string", val);
	}

	@Test
	public void operateList3() {
		// Test out of bounds conditions
		Key key = new Key(args.namespace, args.set, "oplkey3");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get("str1"));
		itemList.add(Value.get("str2"));
		itemList.add(Value.get("str3"));
		itemList.add(Value.get("str4"));
		itemList.add(Value.get("str5"));
		itemList.add(Value.get("str6"));
		itemList.add(Value.get("str7"));

		Record record = client.operate(null, key,
				ListOperation.appendItems(binName, itemList),
				ListOperation.get(binName, 2),
				ListOperation.getRange(binName, 6, 4),
				ListOperation.getRange(binName, -7, 3),
				ListOperation.getRange(binName, 0, 2),
				ListOperation.getRange(binName, -2, 4)
				//ListOperation.get(binName, 7) causes entire command to fail.
				//ListOperation.getRange(binName, 7, 1), causes entire command to fail.
				//ListOperation.getRange(binName, -8, 1) causes entire command to fail.
				//ListOperation.get(binName, -8), causes entire command to fail.
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> list = record.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(7, size);

		assertEquals("str3", (String)list.get(1));

		List<?> rangeList = (List<?>)list.get(2);
		assertEquals(1, rangeList.size());
		assertEquals("str7", (String)rangeList.get(0));

		rangeList = (List<?>)list.get(3);
		assertEquals(3, rangeList.size());
		assertEquals("str1", (String)rangeList.get(0));
		assertEquals("str2", (String)rangeList.get(1));
		assertEquals("str3", (String)rangeList.get(2));

		rangeList = (List<?>)list.get(4);
		assertEquals(2, rangeList.size());
		assertEquals("str1", (String)rangeList.get(0));
		assertEquals("str2", (String)rangeList.get(1));

		rangeList = (List<?>)list.get(5);
		assertEquals(2, rangeList.size());
		assertEquals("str6", (String)rangeList.get(0));
		assertEquals("str7", (String)rangeList.get(1));
	}

	@Test
	public void operateList4() {
		// Test all value types.
		Key key = new Key(args.namespace, args.set, "oplkey4");

		client.delete(null, key);

		List<Value> inputList = new ArrayList<Value>();
		inputList.add(Value.get(12));
		inputList.add(Value.get(-8734.81));
		inputList.add(Value.get("my string"));

		Map<Integer,String> inputMap = new HashMap<Integer,String>();
		inputMap.put(9, "data 9");
		inputMap.put(-2, "data -2");

		byte[] bytes = "string bytes".getBytes();

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(true));
		itemList.add(Value.get(55));
		itemList.add(Value.get("string value"));
		itemList.add(Value.get(inputList));
		itemList.add(Value.get(bytes));
		itemList.add(Value.get(99.99));
		itemList.add(Value.get(inputMap));

		Record record = client.operate(null, key,
				ListOperation.appendItems(binName, itemList),
				ListOperation.getRange(binName, 0, 100),
				ListOperation.set(binName, 1, Value.get("88")),
				ListOperation.get(binName, 1),
				ListOperation.popRange(binName, -2, 1),
				ListOperation.popRange(binName, -1),
				ListOperation.remove(binName, 3),
				ListOperation.removeRange(binName, 0, 1),
				ListOperation.removeRange(binName, 2),
				ListOperation.size(binName)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> list = record.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(7, size);

		List<?> rangeList = (List<?>)list.get(1);
		assertTrue((boolean)(Boolean)rangeList.get(0));
		assertEquals(55, (long)(Long)rangeList.get(1));
		assertEquals("string value", (String)rangeList.get(2));

		List<?> subList = (List<?>)rangeList.get(3);
		assertEquals(3, subList.size());
		assertEquals(12, (long)(Long)subList.get(0));
		assertEquals(-8734.81, (double)(Double)subList.get(1), 0.00001);
		assertEquals("my string", (String)subList.get(2));

		byte[] bt = (byte[])rangeList.get(4);
		assertArrayEquals("bytes not equal", bytes, bt);

		assertEquals(99.99, (double)(Double)rangeList.get(5), 0.00001);

		Map<?,?> subMap = (Map<?,?>)rangeList.get(6);
		assertEquals(2, subMap.size());
		assertEquals("data 9", (String)subMap.get(9L));
		assertEquals("data -2", (String)subMap.get(-2L));

		// Set does not return a result.
		assertEquals("88", (String)list.get(2));

		subList = (List<?>)list.get(3);
		assertEquals(1, subList.size());
		assertEquals(99.99, (double)(Double)subList.get(0), 0.00001);

		subList = (List<?>)list.get(4);
		assertEquals(1, subList.size());
		assertTrue(subList.get(0) instanceof Map);

		assertEquals(1, (long)(Long)list.get(5));
		assertEquals(1, (long)(Long)list.get(6));
		assertEquals(1, (long)(Long)list.get(7));

		size = (Long)list.get(8);
		assertEquals(2, size);
	}

	@Test
	public void operateList5() {
		// Test trim.
		Key key = new Key(args.namespace, args.set, "oplkey5");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get("s11"));
		itemList.add(Value.get("s22222"));
		itemList.add(Value.get("s3333333"));
		itemList.add(Value.get("s4444444444"));
		itemList.add(Value.get("s5555555555555555"));

		Record record = client.operate(null, key,
				ListOperation.insertItems(binName, 0, itemList),
				ListOperation.trim(binName, -5, 5),
				ListOperation.trim(binName, 1, -5),
				ListOperation.trim(binName, 1, 2)
				//ListOperation.trim(binName, 11, 6) causes entire command to fail.
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> list = record.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(5, size);

		size = (Long)list.get(1);
		assertEquals(0, size);

		size = (Long)list.get(2);
		assertEquals(1, size);

		size = (Long)list.get(3);
		assertEquals(2, size);
	}

	@Test
	public void operateList6() {
		// Test clear.
		Key key = new Key(args.namespace, args.set, "oplkey6");

		client.delete(null, key);

		WritePolicy policy = new WritePolicy();
		policy.respondAllOps = true;

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get("s11"));
		itemList.add(Value.get("s22222"));
		itemList.add(Value.get("s3333333"));
		itemList.add(Value.get("s4444444444"));
		itemList.add(Value.get("s5555555555555555"));

		Record record = client.operate(policy, key,
				Operation.put(new Bin("otherbin", 11)),
				Operation.get("otherbin"),
				ListOperation.appendItems(binName, itemList),
				ListOperation.clear(binName),
				ListOperation.size(binName)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> list = record.getList("otherbin");
		assertEquals(2, list.size());
		assertNull(list.get(0));
		assertEquals(11, (long)(Long)list.get(1));

		list = record.getList(binName);

		long size = (Long)list.get(0);
		assertEquals(5, size);

		// clear() does not return value by default, but we set respondAllOps, so it returns null.
		assertNull(list.get(1));

		size = (Long)list.get(2);
		assertEquals(0, size);
	}

	@Test
	public void operateList7() {
		// Test null values.
		Key key = new Key(args.namespace, args.set, "oplkey7");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get("s11"));
		itemList.add(Value.getAsNull());
		itemList.add(Value.get("s3333333"));

		Record record = client.operate(null, key,
				ListOperation.appendItems(binName, itemList),
				ListOperation.get(binName, 0),
				ListOperation.get(binName, 1),
				ListOperation.get(binName, 2)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(3, size);

		String str = (String)results.get(i++);
		assertEquals("s11", str);

		str = (String)results.get(i++);
		assertNull(str);

		str = (String)results.get(i++);
		assertEquals("s3333333", str);
	}

	@Test
	public void operateList8() {
		// Test increment.
		Key key = new Key(args.namespace, args.set, "oplkey8");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(1));
		itemList.add(Value.get(2));
		itemList.add(Value.get(3));

		Record record = client.operate(null, key,
				ListOperation.appendItems(binName, itemList),
				ListOperation.increment(binName, 2),
				ListOperation.increment(ListPolicy.Default, binName, 2),
				ListOperation.increment(binName, 1, Value.get(7)),
				ListOperation.increment(ListPolicy.Default, binName, 1, Value.get(7)),
				ListOperation.get(binName, 0)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(3, size);

		long val = (Long)results.get(i++);
		assertEquals(4, val);

		val = (Long)results.get(i++);
		assertEquals(5, val);

		val = (Long)results.get(i++);
		assertEquals(9, val);

		val = (Long)results.get(i++);
		assertEquals(16, val);

		val = (Long)results.get(i++);
		assertEquals(1, val);
	}

	@Test
	public void operateListSwitchSort() {
		Key key = new Key(args.namespace, args.set, "oplkey9");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(4));
		itemList.add(Value.get(3));
		itemList.add(Value.get(1));
		itemList.add(Value.get(5));
		itemList.add(Value.get(2));

		Record record = client.operate(null, key,
				ListOperation.appendItems(ListPolicy.Default, binName, itemList),
				ListOperation.getByIndex(binName, 3, ListReturnType.VALUE)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(5L, size);

		long val = (Long)results.get(i++);
		assertEquals(5L, val);

		List<Value> valueList = new ArrayList<Value>();
		valueList.add(Value.get(4));
		valueList.add(Value.get(2));

		// Sort list.
		record = client.operate(null, key,
				ListOperation.setOrder(binName, ListOrder.ORDERED),
				ListOperation.getByValue(binName, Value.get(3), ListReturnType.INDEX),
				ListOperation.getByValueRange(binName, Value.get(-1), Value.get(3), ListReturnType.COUNT),
				ListOperation.getByValueList(binName, valueList, ListReturnType.RANK),
				ListOperation.getByIndex(binName, 3, ListReturnType.VALUE),
				ListOperation.getByIndexRange(binName, -2, 2, ListReturnType.VALUE),
				ListOperation.getByRank(binName, 0, ListReturnType.VALUE),
				ListOperation.getByRankRange(binName, 2, 3, ListReturnType.VALUE)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		results = record.getList(binName);
		i = 0;

		List<?> list = (List<?>)results.get(i++);
		assertEquals(2L, list.get(0));

		val = (Long)results.get(i++);
		assertEquals(2L, val);

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(3L, list.get(0));
		assertEquals(1L, list.get(1));

		val = (Long)results.get(i++);
		assertEquals(4L, val);

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));

		val = (Long)results.get(i++);
		assertEquals(1L, val);

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(3L, list.get(0));
		assertEquals(4L, list.get(1));
		assertEquals(5L, list.get(2));
	}

	@Test
	public void operateListSort() {
		Key key = new Key(args.namespace, args.set, "oplkey10");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(-44));
		itemList.add(Value.get(33));
		itemList.add(Value.get(-1));
		itemList.add(Value.get(33));
		itemList.add(Value.get(-2));

		Record record = client.operate(null, key,
				ListOperation.appendItems(ListPolicy.Default, binName, itemList),
				ListOperation.sort(binName, ListSortFlags.DROP_DUPLICATES),
				ListOperation.size(binName)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(5L, size);

		long val = (Long)results.get(i++);
		assertEquals(4L, val);
	}

	@Test
	public void operateListRemove() {
		Key key = new Key(args.namespace, args.set, "oplkey11");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(-44));
		itemList.add(Value.get(33));
		itemList.add(Value.get(-1));
		itemList.add(Value.get(33));
		itemList.add(Value.get(-2));
		itemList.add(Value.get(0));
		itemList.add(Value.get(22));
		itemList.add(Value.get(11));
		itemList.add(Value.get(14));
		itemList.add(Value.get(6));

		List<Value> valueList = new ArrayList<Value>();
		valueList.add(Value.get(-45));
		valueList.add(Value.get(14));

		Record record = client.operate(null, key,
				ListOperation.appendItems(ListPolicy.Default, binName, itemList),
				ListOperation.removeByValue(binName, Value.get(0), ListReturnType.INDEX),
				ListOperation.removeByValueList(binName, valueList, ListReturnType.VALUE),
				ListOperation.removeByValueRange(binName, Value.get(33), Value.get(100), ListReturnType.VALUE),
				ListOperation.removeByIndex(binName, 1, ListReturnType.VALUE),
				ListOperation.removeByIndexRange(binName, 100, 101, ListReturnType.VALUE),
				ListOperation.removeByRank(binName, 0, ListReturnType.VALUE),
				ListOperation.removeByRankRange(binName, 3, 1, ListReturnType.VALUE)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(10L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(5L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(14L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(33L, list.get(0));
		assertEquals(33L, list.get(1));

		long val = (Long)results.get(i++);
		assertEquals(-1L, val);

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		val = (Long)results.get(i++);
		assertEquals(-44L, val);

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(22L, list.get(0));
	}

	@Test
	public void operateListInverted() {
		Key key = new Key(args.namespace, args.set, "oplkey12");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(4));
		itemList.add(Value.get(3));
		itemList.add(Value.get(1));
		itemList.add(Value.get(5));
		itemList.add(Value.get(2));

		List<Value> valueList = new ArrayList<Value>();
		valueList.add(Value.get(4));
		valueList.add(Value.get(2));

		Record record = client.operate(null, key,
				ListOperation.appendItems(ListPolicy.Default, binName, itemList),
				ListOperation.getByValue(binName, Value.get(3), ListReturnType.INDEX | ListReturnType.INVERTED),
				ListOperation.getByValueRange(binName, Value.get(-1), Value.get(3), ListReturnType.COUNT | ListReturnType.INVERTED),
				ListOperation.getByValueList(binName, valueList, ListReturnType.RANK | ListReturnType.INVERTED),
				ListOperation.getByIndexRange(binName, -2, 2, ListReturnType.VALUE | ListReturnType.INVERTED),
				ListOperation.getByRankRange(binName, 2, 3, ListReturnType.VALUE | ListReturnType.INVERTED)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(5L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(2L, list.get(1));
		assertEquals(3L, list.get(2));
		assertEquals(4L, list.get(3));

		long val = (Long)results.get(i++);
		assertEquals(3L, val);

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(2L, list.get(1));
		assertEquals(4L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(3L, list.get(1));
		assertEquals(1L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(1L, list.get(0));
		assertEquals(2L, list.get(1));
	}

	@Test
	public void operateListGetRelative() {
		Key key = new Key(args.namespace, args.set, "oplkey13");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(0));
		itemList.add(Value.get(4));
		itemList.add(Value.get(5));
		itemList.add(Value.get(9));
		itemList.add(Value.get(11));
		itemList.add(Value.get(15));

		Record record = client.operate(null, key,
				ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT), binName, itemList),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(5), 0, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(5), 1, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(5), -1, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(3), 0, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(3), 3, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(3), -3, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(5), 0, 2, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(5), 1, 1, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(5), -1, 2, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(3), 0, 1, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(3), 3, 7, ListReturnType.VALUE),
				ListOperation.getByValueRelativeRankRange(binName, Value.get(3), -3, 2, ListReturnType.VALUE)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(6L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());
		assertEquals(5L, list.get(0));
		assertEquals(9L, list.get(1));
		assertEquals(11L, list.get(2));
		assertEquals(15L, list.get(3));

		list = (List<?>)results.get(i++);
		assertEquals(3L, list.size());
		assertEquals(9L, list.get(0));
		assertEquals(11L, list.get(1));
		assertEquals(15L, list.get(2));

		list = (List<?>)results.get(i++);
		assertEquals(5L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));
		assertEquals(9L, list.get(2));
		assertEquals(11L, list.get(3));
		assertEquals(15L, list.get(4));

		list = (List<?>)results.get(i++);
		assertEquals(5L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));
		assertEquals(9L, list.get(2));
		assertEquals(11L, list.get(3));
		assertEquals(15L, list.get(4));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(11L, list.get(0));
		assertEquals(15L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(6L, list.size());
		assertEquals(0L, list.get(0));
		assertEquals(4L, list.get(1));
		assertEquals(5L, list.get(2));
		assertEquals(9L, list.get(3));
		assertEquals(11L, list.get(4));
		assertEquals(15L, list.get(5));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(5L, list.get(0));
		assertEquals(9L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(9L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(4L, list.get(0));
		assertEquals(5L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(4L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(2L, list.size());
		assertEquals(11L, list.get(0));
		assertEquals(15L, list.get(1));

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());
	}

	@Test
	public void operateListRemoveRelative() {
		Key key = new Key(args.namespace, args.set, "oplkey14");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(0));
		itemList.add(Value.get(4));
		itemList.add(Value.get(5));
		itemList.add(Value.get(9));
		itemList.add(Value.get(11));
		itemList.add(Value.get(15));

		Record record = client.operate(null, key,
				ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT), binName, itemList),
				ListOperation.removeByValueRelativeRankRange(binName, Value.get(5), 0, ListReturnType.VALUE),
				ListOperation.removeByValueRelativeRankRange(binName, Value.get(5), 1, ListReturnType.VALUE),
				ListOperation.removeByValueRelativeRankRange(binName, Value.get(5), -1, ListReturnType.VALUE),
				ListOperation.removeByValueRelativeRankRange(binName, Value.get(3), -3, 1, ListReturnType.VALUE),
				ListOperation.removeByValueRelativeRankRange(binName, Value.get(3), -3, 2, ListReturnType.VALUE),
				ListOperation.removeByValueRelativeRankRange(binName, Value.get(3), -3, 3, ListReturnType.VALUE)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long size = (Long)results.get(i++);
		assertEquals(6L, size);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4L, list.size());
		assertEquals(5L, list.get(0));
		assertEquals(9L, list.get(1));
		assertEquals(11L, list.get(2));
		assertEquals(15L, list.get(3));

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(4L, list.get(0));

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(0L, list.size());

		list = (List<?>)results.get(i++);
		assertEquals(1L, list.size());
		assertEquals(0L, list.get(0));
	}

	@Test
	public void operateListPartial() {
		Key key = new Key(args.namespace, args.set, "oplkey15");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(0));
		itemList.add(Value.get(4));
		itemList.add(Value.get(5));
		itemList.add(Value.get(9));
		itemList.add(Value.get(9));
		itemList.add(Value.get(11));
		itemList.add(Value.get(15));
		itemList.add(Value.get(0));

		Record record = client.operate(null, key,
				ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.PARTIAL | ListWriteFlags.NO_FAIL), binName, itemList),
				ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL), "bin2", itemList)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		long size = record.getLong(binName);
		assertEquals(6L, size);

		size = record.getLong("bin2");
		assertEquals(0L, size);

		itemList = new ArrayList<Value>();
		itemList.add(Value.get(11));
		itemList.add(Value.get(3));

		record = client.operate(null, key,
				ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.PARTIAL | ListWriteFlags.NO_FAIL), binName, itemList),
				ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL), "bin2", itemList)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		size = record.getLong(binName);
		assertEquals(7L, size);

		size = record.getLong("bin2");
		assertEquals(2L, size);
	}

	@Test
	public void operateListInfinity() {
		Key key = new Key(args.namespace, args.set, "oplkey16");

		client.delete(null, key);

		List<Value> itemList = new ArrayList<Value>();
		itemList.add(Value.get(0));
		itemList.add(Value.get(4));
		itemList.add(Value.get(5));
		itemList.add(Value.get(9));
		itemList.add(Value.get(11));
		itemList.add(Value.get(15));

		Record record = client.operate(null, key,
				ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT), binName, itemList)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		long size = record.getLong(binName);
		assertEquals(6L, size);

		itemList = new ArrayList<Value>();
		itemList.add(Value.get(11));
		itemList.add(Value.get(3));

		record = client.operate(null, key,
				ListOperation.getByValueRange(binName, Value.get(10), Value.INFINITY, ListReturnType.VALUE)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long val = (Long)results.get(i++);
		assertEquals(11L, val);

		val = (Long)results.get(i++);
		assertEquals(15L, val);
	}

	@Test
	public void operateListWildcard() {
		Key key = new Key(args.namespace, args.set, "oplkey17");

		client.delete(null, key);

		List<Value> i1 = new ArrayList<Value>();
		i1.add(Value.get("John"));
		i1.add(Value.get(55));

		List<Value> i2 = new ArrayList<Value>();
		i2.add(Value.get("Jim"));
		i2.add(Value.get(95));

		List<Value> i3 = new ArrayList<Value>();
		i3.add(Value.get("Joe"));
		i3.add(Value.get(80));

		List<Value> itemList = new ArrayList<Value>();

		itemList.add(Value.get(i1));
		itemList.add(Value.get(i2));
		itemList.add(Value.get(i3));

		Record record = client.operate(null, key,
				ListOperation.appendItems(binName, itemList)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		long size = record.getLong(binName);
		assertEquals(3L, size);

		itemList = new ArrayList<Value>();
		itemList.add(Value.get("Jim"));
		itemList.add(Value.WILDCARD);

		record = client.operate(null, key,
				ListOperation.getByValue(binName, Value.get(itemList), ListReturnType.VALUE)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		List<?> items = (List<?>)results.get(i++);
		String s = (String)items.get(0);
		assertEquals("Jim", s);

		long v = (Long)items.get(1);
		assertEquals(95L, v);
	}

	@Test
	public void operateNestedList() {
		Key key = new Key(args.namespace, args.set, "oplkey18");

		client.delete(null, key);

		List<Value> l1 = new ArrayList<Value>();
		l1.add(Value.get(7));
		l1.add(Value.get(9));
		l1.add(Value.get(5));

		List<Value> l2 = new ArrayList<Value>();
		l2.add(Value.get(1));
		l2.add(Value.get(2));
		l2.add(Value.get(3));

		List<Value> l3 = new ArrayList<Value>();
		l3.add(Value.get(6));
		l3.add(Value.get(5));
		l3.add(Value.get(4));
		l3.add(Value.get(1));

		List<Value> inputList = new ArrayList<Value>();
		inputList.add(Value.get(l1));
		inputList.add(Value.get(l2));
		inputList.add(Value.get(l3));

		// Create list.
		client.put(null, key, new Bin(binName, inputList));

		// Append value to last list and retrieve all lists.
		Record record = client.operate(null, key,
				ListOperation.append(binName, Value.get(11), CTX.listIndex(-1)),
				Operation.get(binName)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(5, count);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(3, list.size());

		// Test last nested list.
		list = (List<?>)list.get(2);
		assertEquals(5, list.size());
		assertEquals(6, (long)(Long)list.get(0));
		assertEquals(5, (long)(Long)list.get(1));
		assertEquals(4, (long)(Long)list.get(2));
		assertEquals(1, (long)(Long)list.get(3));
		assertEquals(11, (long)(Long)list.get(4));
	}

	@Test
	public void operateNestedListMap() {
		Key key = new Key(args.namespace, args.set, "oplkey19");

		client.delete(null, key);

		List<Value> l11 = new ArrayList<Value>();
		l11.add(Value.get(7));
		l11.add(Value.get(9));
		l11.add(Value.get(5));

		List<Value> l12 = new ArrayList<Value>();
		l12.add(Value.get(13));

		List<Value> l1 = new ArrayList<Value>();
		l1.add(Value.get(l11));
		l1.add(Value.get(l12));

		List<Value> l21 = new ArrayList<Value>();
		l21.add(Value.get(9));

		List<Value> l22 = new ArrayList<Value>();
		l22.add(Value.get(2));
		l22.add(Value.get(4));

		List<Value> l23 = new ArrayList<Value>();
		l23.add(Value.get(6));
		l23.add(Value.get(1));
		l23.add(Value.get(9));

		List<Value> l2 = new ArrayList<Value>();
		l2.add(Value.get(l21));
		l2.add(Value.get(l22));
		l2.add(Value.get(l23));

		Map<Value,Value> inputMap = new HashMap<Value,Value>();
		inputMap.put(Value.get("key1"), Value.get(l1));
		inputMap.put(Value.get("key2"), Value.get(l2));

		// Create list.
		client.put(null, key, new Bin(binName, inputMap));

		// Append value to last list and retrieve map.
		Record record = client.operate(null, key,
				ListOperation.append(binName, Value.get(11), CTX.mapKey(Value.get("key2")), CTX.listRank(0)),
				Operation.get(binName)
				);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(3, count);

		Map<?,?> map = (Map<?,?>)results.get(i++);
		assertEquals(2, map.size());

		// Test affected nested list.
		List<?> list = (List<?>)map.get("key2");
		assertEquals(3, list.size());

		list = (List<?>)list.get(1);
		assertEquals(3, list.size());
		assertEquals(2, (long)(Long)list.get(0));
		assertEquals(4, (long)(Long)list.get(1));
		assertEquals(11, (long)(Long)list.get(2));
	}

	@Test
	public void operateListCreateContext() {
		Key key = new Key(args.namespace, args.set, "oplkey20");

		client.delete(null, key);

		List<Value> l1 = new ArrayList<Value>();
		l1.add(Value.get(7));
		l1.add(Value.get(9));
		l1.add(Value.get(5));

		List<Value> l2 = new ArrayList<Value>();
		l2.add(Value.get(1));
		l2.add(Value.get(2));
		l2.add(Value.get(3));

		List<Value> l3 = new ArrayList<Value>();
		l3.add(Value.get(6));
		l3.add(Value.get(5));
		l3.add(Value.get(4));
		l3.add(Value.get(1));

		List<Value> inputList = new ArrayList<Value>();
		inputList.add(Value.get(l1));
		inputList.add(Value.get(l2));
		inputList.add(Value.get(l3));

		// Create list.
		Record record = client.operate(null, key,
			ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED, 0), binName, inputList),
			Operation.get(binName)
		);
		//System.out.println("Record: " + record);

		// Append value to new list created after the original 3 lists.
		record = client.operate(null, key,
			ListOperation.append(binName, Value.get(2), CTX.listIndexCreate(3, ListOrder.ORDERED, false)),
			//ListOperation.create(binName, ListOrder.ORDERED, false, CTX.listIndex(3)),
			//ListOperation.append(binName, Value.get(2), CTX.listIndex(3)),
			Operation.get(binName)
		);

		assertRecordFound(key, record);
		//System.out.println("Record: " + record);

		List<?> results = record.getList(binName);
		int i = 0;

		long count = (Long)results.get(i++);
		assertEquals(1, count);

		List<?> list = (List<?>)results.get(i++);
		assertEquals(4, list.size());

		// Test last nested list.
		list = (List<?>)list.get(1);
		assertEquals(1, list.size());
		assertEquals(2, (long)(Long)list.get(0));
	}
}
