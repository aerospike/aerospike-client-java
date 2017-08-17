/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package com.aerospike.test.sync.large;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;

public class TestLargeList extends TestSync {
	private static final String binName = args.getBinName("ListBin");

	@BeforeClass
	public static void register() {
		RegisterTask task = client.register(null, TestLargeList.class.getClassLoader(), "udf/largelist_example.lua", "largelist_example.lua", Language.LUA);
		task.waitTillComplete();
	}

	@Test
	public void simpleLargeList() {
		if (! args.validateLDT()) {
			return;
		}
		Key key = new Key(args.namespace, args.set, "setkey");
		
		// Delete record if it already exists.
		client.delete(null, key);
		
		// Initialize large set operator.
		LargeList llist = client.getLargeList(null, key, binName);
		String orig1 = "llistValue1";
		String orig2 = "llistValue2";
		String orig3 = "llistValue3";
						
		// Write values.
		llist.add(Value.get(orig1));
		llist.add(Value.get(orig2));
		llist.add(Value.get(orig3));
		
		// Perform exists.
		boolean b = llist.exists(Value.get(orig2));
		assertTrue(b);
		
		b = llist.exists(Value.get("notfound"));
		assertFalse(b);
		
		// Test record not found.
		LargeList nflist = client.getLargeList(null, new Key(args.namespace, args.set, "sfdfdqw"), binName);
		try {
			b = nflist.exists(Value.get(orig2));
			assertFalse(b);
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, ae.getResultCode());
		}

		List<Value> klist = new ArrayList<Value>();
		klist.add(Value.get(orig2));
		klist.add(Value.get(orig1));
		klist.add(Value.get("notfound"));
		List<Boolean> blist = llist.exists(klist);
		assertTrue(blist.get(0));
		assertTrue(blist.get(1));
		assertFalse(blist.get(2));
		
		// Test record not found.
		try {
			List<Boolean> blist2 = nflist.exists(klist);
			assertFalse(blist2.get(0));
			assertFalse(blist2.get(1));
			assertFalse(blist2.get(2));
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.KEY_NOT_FOUND_ERROR, ae.getResultCode());
		}

		// Perform a Range Query -- look for "llistValue2" to "llistValue3"
		List<?> rangeList = llist.range(Value.get(orig2), Value.get(orig3));
		assertNotNull(rangeList);
		assertEquals(2, rangeList.size());

		String v2 = (String) rangeList.get(0);
		String v3 = (String) rangeList.get(1);
		assertEquals(orig2, v2);
		assertEquals(orig3, v3);
		
		// Remove last value.
		llist.remove(Value.get(orig3));
		
		int size = llist.size();
		assertEquals(2, size);
		
		List<?> listReceived = llist.find(Value.get(orig2));
		String expected = orig2;
		
		assertNotNull(listReceived);
		
		String stringReceived = (String) listReceived.get(0);
		assertNotNull(stringReceived);	
		assertEquals(expected, stringReceived);
	}
	
	@Test
	public void filterLargeList() {
		if (! args.validateLDT()) {
			return;
		}
		Key key = new Key(args.namespace, args.set, "setkey");
		
		// Delete record if it already exists.
		client.delete(null, key);
		
		// Initialize large set operator.
		LargeList llist = client.getLargeList(null, key, binName);
		int orig1 = 1;
		int orig2 = 2;
		int orig3 = 3;
		int orig4 = 4;
						
		// Write values.
		llist.add(Value.get(orig1), Value.get(orig2), Value.get(orig3), Value.get(orig4));
		
		// Filter on values
		List<?> filterList = llist.filter("largelist_example", "my_filter_func", Value.get(orig3));
		assertNotNull(filterList);	
		assertEquals(1, filterList.size());
				
		Long v = (Long)filterList.get(0);
		assertEquals(orig3, v.intValue());		
	}

	@Test
	@SuppressWarnings("unchecked")
	public void distinctBinsLargeList() {
		if (! args.validateLDT()) {
			return;
		}
		Key key = new Key(args.namespace, args.set, "accountId");

		// Delete record if it already exists.
		client.delete(null, key);	

		// Initialize large list operator.
		LargeList list = client.getLargeList(null, key, "trades");

		// Write trades
		Map<String,Value> map = new HashMap<String,Value>();

		Calendar timestamp1 = new GregorianCalendar(2014, 6, 25, 12, 18, 43);	
		map.put("key", Value.get(timestamp1.getTimeInMillis()));
		map.put("ticker", Value.get("IBM"));
		map.put("qty", Value.get(100));
		map.put("price", Value.get(Double.doubleToLongBits(181.82)));
		list.add(Value.get(map));

		Calendar timestamp2 = new GregorianCalendar(2014, 6, 26, 9, 33, 17);
		map.put("key", Value.get(timestamp2.getTimeInMillis()));
		map.put("ticker", Value.get("GE"));
		map.put("qty", Value.get(500));
		map.put("price", Value.get(Double.doubleToLongBits(26.36)));
		list.add(Value.get(map));

		Calendar timestamp3 = new GregorianCalendar(2014, 6, 27, 14, 40, 19);
		map.put("key", Value.get(timestamp3.getTimeInMillis()));
		map.put("ticker", Value.get("AAPL"));
		map.put("qty", Value.get(75));
		map.put("price", Value.get(Double.doubleToLongBits(91.85)));
		list.add(Value.get(map));

		// Verify list size
		int size = list.size();
		assertEquals(3, size);

		// Filter on range of timestamps
		Calendar begin = new GregorianCalendar(2014, 6, 26);
		Calendar end = new GregorianCalendar(2014, 6, 28);
		List<Map<String,Object>> results = (List<Map<String,Object>>)list.range(Value.get(begin.getTimeInMillis()), Value.get(end.getTimeInMillis()));
		assertNotNull(results);	
		assertEquals(2, results.size());

		// Verify data.
		validateWithDistinctBins(results, 0, timestamp2, "GE", 500, 26.36);
		validateWithDistinctBins(results, 1, timestamp3, "AAPL", 75, 91.85);
		
		List<Map<String,Object>> rows = (List<Map<String,Object>>)list.scan();
		for (Map<String,Object> row : rows) {
			for (@SuppressWarnings("unused") Map.Entry<String,Object> entry : row.entrySet()) {
				//console.Info(entry.Key.ToString());
				//console.Info(entry.Value.ToString());
			}
		}
	}

	private void validateWithDistinctBins(List<Map<String,Object>> list, int index, Calendar expectedTime, String expectedTicker, int expectedQty, double expectedPrice) {		
		Map<String,Object> map = list.get(index);
		Calendar receivedTime = new GregorianCalendar();
		receivedTime.setTimeInMillis((Long)map.get("key"));

		assertEquals(expectedTime, receivedTime);
		assertEquals(expectedTicker, (String)map.get("ticker"));
		assertEquals(expectedQty, ((Long)map.get("qty")).intValue());
		assertEquals(expectedPrice, Double.longBitsToDouble((Long)map.get("price")), 0.000001);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void runWithSerializedBin() throws IOException {
		if (! args.validateLDT()) {
			return;
		}
		Key key = new Key(args.namespace, args.set, "accountId");

		// Delete record if it already exists.
		client.delete(null, key);

		// Initialize large list operator.
		LargeList list = client.getLargeList(null, key, "trades");

		// Write trades
		Map<String, Value> map = new HashMap<String, Value>();
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream(500);
		DataOutputStream writer = new DataOutputStream(byteStream);

		Calendar timestamp1 = new GregorianCalendar(2014, 6, 25, 12, 18, 43);
		map.put("key", Value.get(timestamp1.getTimeInMillis()));
		writer.writeUTF("IBM");     // ticker
		writer.writeInt(100);       // qty
		writer.writeDouble(181.82); // price
		map.put("value", Value.get(byteStream.toByteArray()));
		list.add(Value.get(map));

		Calendar timestamp2 = new GregorianCalendar(2014, 6, 26, 9, 33, 17);
		map.put("key", Value.get(timestamp2.getTimeInMillis()));
		byteStream.reset();
		writer.writeUTF("GE");     // ticker
		writer.writeInt(500);      // qty
		writer.writeDouble(26.36); // price
		map.put("value", Value.get(byteStream.toByteArray()));
		list.add(Value.get(map));

		Calendar timestamp3 = new GregorianCalendar(2014, 6, 27, 14, 40, 19);
		map.put("key", Value.get(timestamp3.getTimeInMillis()));
		byteStream.reset();
		writer.writeUTF("AAPL");   // ticker
		writer.writeInt(75);       // qty
		writer.writeDouble(91.85); // price
		map.put("value", Value.get(byteStream.toByteArray()));
		list.add(Value.get(map));

		// Verify list size
		int size = list.size();
		assertEquals(3, size);

		// Filter on range of timestamps
		Calendar begin = new GregorianCalendar(2014, 6, 26);
		Calendar end = new GregorianCalendar(2014, 6, 28);
		List<Map<String,Object>> results = (List<Map<String,Object>>)list.range(Value.get(begin.getTimeInMillis()), Value.get(end.getTimeInMillis()));
		assertNotNull(results);	
		assertEquals(2, results.size());

		// Verify data.
		validateWithSerializedBin(results, 0, timestamp2, "GE", 500, 26.36);
		validateWithSerializedBin(results, 1, timestamp3, "AAPL", 75, 91.85);
	}

	private void validateWithSerializedBin(List<Map<String,Object>> list, int index, Calendar expectedTime, String expectedTicker, int expectedQty, double expectedPrice) 
		throws IOException {	
		Map<String,Object> map = list.get(index);
		Calendar receivedTime = new GregorianCalendar();
		receivedTime.setTimeInMillis((Long)map.get("key"));

		assertEquals(expectedTime, receivedTime);

		byte[] value = (byte[])map.get("value");
		ByteArrayInputStream ms = new ByteArrayInputStream(value);
		DataInputStream reader = new DataInputStream(ms);
		String receivedTicker = reader.readUTF();
		int receivedQty = reader.readInt();
		double receivedPrice = reader.readDouble();

		assertEquals(expectedTicker, receivedTicker);
		assertEquals(expectedQty, receivedQty);
		assertEquals(expectedPrice, receivedPrice, 0.000001);
	}
	
	@Test
	public void runVolumeInsert() {
		if (! args.validateLDT()) {
			return;
		}
		// This key has already been created in runSimpleExample().
		Key key = new Key(args.namespace, args.set, "setkey");
		
		int itemCount = 2000;
		LargeList llist2 = client.getLargeList(null, key, "NumberBin");
		for (int i = itemCount; i > 0; i-- ){
			llist2.add(Value.get(i));
		}
		assertEquals(2000, llist2.size());
	}
}
