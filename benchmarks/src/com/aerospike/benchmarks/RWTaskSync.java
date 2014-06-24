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

	protected void get(int keyIdx, Key key, String binName) throws AerospikeException {
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
		
		if (record == null && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();	
		}
		else {
			counters.read.count.getAndIncrement();		
		}

		if (args.validate) {
			validateRead(keyIdx, record);
		}
	}
	
	protected void get(int keyIdx, Key key) throws AerospikeException {
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
	
		if (record == null && args.reportNotFound) {
			counters.readNotFound.getAndIncrement();	
		}
		else {
			counters.read.count.getAndIncrement();		
		}

		if (args.validate) {
			validateRead(keyIdx, record);
		}
	}
}
