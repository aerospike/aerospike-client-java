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

	private void addLog(AerospikeClient client, Key key, long timestamp, Value logValue) throws AerospikeException {

		com.aerospike.client.large.LargeList list    = client.getLargeList(null, key, "listltracker", null);
		com.aerospike.client.large.LargeStack lstack = client.getLargeStack(null, key, "stackltracker", null);

		// Create a Entry
		Map<String,Value> log_entry = new HashMap<String,Value>();
		log_entry.put("key", Value.get(timestamp));
		log_entry.put("log", logValue);

		if (args.storeType == Storetype.LLIST) {
			list.add(Value.getAsMap(log_entry));
		} else if (args.storeType == Storetype.LSTACK) {
			lstack.push(Value.getAsMap(log_entry));
		} else {
			System.out.println("Unknown type");
		}
		return;
	}

	protected void list_add(Key key, Value value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			addLog(client, key, begin, value);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			addLog(client, key, begin, value);
			counters.write.count.getAndIncrement();			
		}
	}

	protected void lstack_push(Key key, Value value) throws AerospikeException {
		long begin = System.currentTimeMillis();
		if (counters.write.latency != null) {
			addLog(client, key, begin, value);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			addLog(client, key, begin, value);
			counters.write.count.getAndIncrement();			
		}
	}

	private List<Map<String,Object>> getLogs(AerospikeClient client, Key key, long starttime, long endtime) {
		com.aerospike.client.large.LargeList list = client.getLargeList(null, key, "listltracker", null);
		List<Map<String,Object>> results = null;
		results = (List<Map<String,Object>>)list.range(Value.get(starttime), Value.get(endtime));
		return results;
	}

	private List<Map<String,Object>> getStackLogs(AerospikeClient client, Key key, int numelements) {
		com.aerospike.client.large.LargeStack lstack = client.getLargeStack(null, key, "stackltracker", null);
		List<Map<String,Object>> results = null;
		results = (List<Map<String,Object>>)lstack.peek(numelements);
		return results;
	}


	protected void list_get(Key key) throws AerospikeException {
		List<Map<String,Object>> result;
		long begin = System.currentTimeMillis();
		if (counters.read.latency != null) {
			result = getLogs(client, key, 1000, begin);  
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.count.getAndIncrement();		
			counters.read.latency.add(elapsed);
		}
		else {
			result = getLogs(client, key, 1000, begin);  
		}
	}

	protected void lstack_peek(Key key) throws AerospikeException {
		List<Map<String,Object>> result;
		long begin = System.currentTimeMillis();
		if (counters.read.latency != null) {
			result = getStackLogs(client, key, 1);  
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.count.getAndIncrement();		
			counters.read.latency.add(elapsed);
		}
		else {
			result = getStackLogs(client, key, 1);  
		}
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

}
