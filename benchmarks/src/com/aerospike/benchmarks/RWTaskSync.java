/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.benchmarks;

import java.util.concurrent.atomic.AtomicIntegerArray;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Synchronous read/write task.
 */
public class RWTaskSync extends RWTask {

	public RWTaskSync(
		AerospikeClient client, 
		String namespace,
		String setName,
		int nKeys, 
		int startKey, 
		int keySize, 
		DBObjectSpec[] objects, 
		int nBins, 
		String cycleType,
		Policy readPolicy,
		WritePolicy writePolicy, 
		AtomicIntegerArray settingsArr, 
		boolean validate, 
		CounterStore counters, 
		boolean debug
	) {
		super(client, namespace, setName, nKeys, startKey, keySize, objects, nBins, cycleType, readPolicy, writePolicy, settingsArr, validate, counters, debug);		
	}
		
	protected void put(Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
			client.put(writePolicy, key, bins);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.put(writePolicy, key, bins);
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
			record = client.get(readPolicy, key, binName);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.count.getAndIncrement();			
			counters.read.latency.add(elapsed);
		}
		else {
			record = client.get(readPolicy, key, binName);
			counters.read.count.getAndIncrement();
		}

		if (this.validate) {
			validateRead(keyIdx, record);
		}
	}
	
	protected void get(int keyIdx, Key key) throws AerospikeException {
		Record record;
		
		if (counters.read.latency != null) {
			long begin = System.currentTimeMillis();
			record = client.get(readPolicy, key);
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.count.getAndIncrement();			
			counters.read.latency.add(elapsed);
		}
		else {
			record = client.get(readPolicy, key);
			counters.read.count.getAndIncrement();
		}
	
		if (this.validate) {
			validateRead(keyIdx, record);
		}
	}
}
