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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

/**
 * Asynchronous read/write task.
 */
public class RWTaskAsync extends RWTask {

	private final AsyncClient client;
	private final WriteHandler writeHandler;
	private final ReadHandler readHandler;
	
	public RWTaskAsync(
		AsyncClient client, 
		String namespace,
		String setName,
		int nKeys, 
		int startKey, 
		int keySize, 
		DBObjectSpec[] objects, 
		int nBins, 
		String cycleType, 
		WritePolicy policy, 
		AtomicIntegerArray settingsArr, 
		boolean validate, 
		int runTime, 
		CounterStore counters, 
		boolean debug
	) {
		super(client, namespace, setName, nKeys, startKey, keySize, objects, nBins, cycleType, policy, settingsArr, validate, runTime, counters, debug);
		this.client = client;
		writeHandler = new WriteHandler();
		readHandler = new ReadHandler();
	}
		
	protected void put(Key key, Bin[] bins) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.write.timeouts.get() > 0) {
			Thread.yield();
		}

		if (counters.write.latency != null) {
			client.put(policy, new LatencyWriteHandler(), key, bins);	
		}
		else {
			client.put(policy, writeHandler, key, bins);
		}
	}
		
	protected void add(Key key, Bin[] bins) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.write.timeouts.get() > 0) {
			Thread.yield();
		}
		
		if (counters.write.latency != null) {
			client.add(writePolicyGeneration, new LatencyWriteHandler(), key, bins);
		}
		else {
			client.add(writePolicyGeneration, writeHandler, key, bins);			
		}
	}
	
	protected void get(int keyIdx, Key key, String binName) throws AerospikeException {		
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.read.timeouts.get() > 0) {
			Thread.yield();
		}
		
		if (counters.read.latency != null) {		
			client.get(policy, new LatencyReadHandler(), key, binName);
		}
		else {			
			client.get(policy, readHandler, key, binName);
		}
	}
	
	protected void get(int keyIdx, Key key) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.read.timeouts.get() > 0) {
			Thread.yield();
		}

		if (counters.read.latency != null) {	
			client.get(policy, new LatencyReadHandler(), key);
		}
		else {			
			client.get(policy, readHandler, key);
		}
	}
	
	private final class WriteHandler implements WriteListener {		
		@Override
		public void onSuccess(Key key) {
			counters.write.count.getAndIncrement();
		}
	
		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
		}
	}

	private final class ReadHandler implements RecordListener {
		@Override
		public void onSuccess(Key key, Record record) {
			counters.read.count.getAndIncrement();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
		}
	}
	
	private final class LatencyWriteHandler implements WriteListener {
		private long begin;
		
		public LatencyWriteHandler() {
			this.begin = System.currentTimeMillis();
		}
		
		@Override
		public void onSuccess(Key key) {
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
		}		
	}
	
	private final class LatencyReadHandler implements RecordListener {
		private long begin;
		
		public LatencyReadHandler() {
			this.begin = System.currentTimeMillis();
		}
		
		@Override
		public void onSuccess(Key key, Record record) {
			long elapsed = System.currentTimeMillis() - begin;
			counters.read.count.getAndIncrement();			
			counters.read.latency.add(elapsed);
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
		}		
	}
	
	/*
	private final class ValidateHandler implements RecordListener {
		
		private final int keyIdx;
		
		public ValidateHandler(int keyIdx) {
			this.keyIdx = keyIdx;
		}
		
		@Override
		public void onSuccess(Key key, Record record) {
			counters.read.count.getAndIncrement();
			validateRead(keyIdx, record);
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
		}
	}*/
}
