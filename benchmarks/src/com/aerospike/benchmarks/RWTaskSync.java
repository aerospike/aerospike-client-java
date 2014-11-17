/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
package com.aerospike.benchmarks;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeStack;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
/**
 * Synchronous read/write task.
 */
public class RWTaskSync extends RWTask {

	public RWTaskSync(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(client, args, counters, keyStart, keyCount);	
	}
		
	protected void put(Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
			client.put(args.writePolicy, key, bins);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.put(args.writePolicy, key, bins);
			counters.write.count.getAndIncrement();			
		}
	}
	
	protected void add(Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
			client.add(writePolicyGeneration, key, bins);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.add(writePolicyGeneration, key, bins);
			counters.write.count.getAndIncrement();
		}
	}

	protected void largeListAdd(Key key, Value value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			largeListAdd(key, value, begin);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			largeListAdd(key, value, begin);
			counters.write.count.getAndIncrement();			
		}
	}

	private void largeListAdd(Key key, Value value, long timestamp) throws AerospikeException {
		// Create entry
		Map<String,Value> entry = new HashMap<String,Value>();
		entry.put("key", Value.get(timestamp));
		entry.put("log", value);

		// Add entry
		LargeList list = client.getLargeList(args.writePolicy, key, "listltracker", null);
		list.add(Value.getAsMap(entry));
	}

	protected void largeStackPush(Key key, Value value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			largeStackPush(key, value, begin);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			largeStackPush(key, value, begin);
			counters.write.count.getAndIncrement();			
		}
	}

	private void largeStackPush(Key key, Value value, long timestamp) throws AerospikeException {
		// Create entry
		Map<String,Value> entry = new HashMap<String,Value>();
		entry.put("key", Value.get(timestamp));
		entry.put("log", value);

		// Push entry
		LargeStack lstack = client.getLargeStack(args.writePolicy, key, "stackltracker", null);
		lstack.push(Value.getAsMap(entry));
	}

	protected void get(Key key, String binName) throws AerospikeException {
		Record record;
		
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			record = client.get(args.readPolicy, key, binName);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			record = client.get(args.readPolicy, key, binName);
		}		
		processRead(key, record);
	}
	
	protected void get(Key key) throws AerospikeException {
		Record record;
		
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			record = client.get(args.readPolicy, key);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			record = client.get(args.readPolicy, key);
		}	
		processRead(key, record);
	}
	
	protected void get(Key[] keys, String binName) throws AerospikeException {
		Record[] records;
		
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			records = client.get(args.batchPolicy, keys, binName);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			records = client.get(args.batchPolicy, keys, binName);
		}
	
		for (int i = 0; i < keys.length; i++) {
			processRead(keys[i], records[i]);
		}
	}

	protected void get(Key[] keys) throws AerospikeException {
		Record[] records;
		
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			records = client.get(args.batchPolicy, keys);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			records = client.get(args.batchPolicy, keys);
		}
	
		for (int i = 0; i < keys.length; i++) {
			processRead(keys[i], records[i]);
		}
	}
	
	protected void largeListGet(Key key) throws AerospikeException {
		LargeList list = client.getLargeList(args.writePolicy, key, "listltracker", null);
		List<?> results;
		long begin = System.currentTimeMillis();
		if (counters.read.latency != null) {
			results = list.range(Value.get(1000), Value.get(begin));
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			results = list.range(Value.get(1000), Value.get(begin));
		}
		processLargeRead(key, results);
	}

	protected void largeStackPeek(Key key) throws AerospikeException {
		LargeStack lstack = client.getLargeStack(args.writePolicy, key, "stackltracker", null);
		List<?> results;
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			results = lstack.peek(1);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.latency.add(elapsed);
		}
		else {
			results = lstack.peek(1);
		}
		processLargeRead(key, results);
	}
}
