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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;

/**
 * Asynchronous read/write task.
 */
public class RWTaskAsync extends RWTask {

	private final AsyncClient client;
	private final WriteHandler writeHandler;
	private final ReadHandler readHandler;
	
	public RWTaskAsync(AsyncClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(client, args, counters, keyStart, keyCount);
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
			client.put(args.writePolicy, new LatencyWriteHandler(), key, bins);	
		}
		else {
			client.put(args.writePolicy, writeHandler, key, bins);
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
			client.get(args.readPolicy, new LatencyReadHandler(), key, binName);
		}
		else {			
			client.get(args.readPolicy, readHandler, key, binName);
		}
	}
	
	protected void get(int keyIdx, Key key) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.read.timeouts.get() > 0) {
			Thread.yield();
		}

		if (counters.read.latency != null) {	
			client.get(args.readPolicy, new LatencyReadHandler(), key);
		}
		else {			
			client.get(args.readPolicy, readHandler, key);
		}
	}

	protected void get(int keyIdx, int count, Key keys[]) throws AerospikeException {
		// No Op right now.
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
			if (record == null && args.reportNotFound) {
				counters.readNotFound.getAndIncrement();	
			}
			else {
				counters.read.count.getAndIncrement();		
			}
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
			counters.read.latency.add(elapsed);
			
			if (record == null && args.reportNotFound) {
				counters.readNotFound.getAndIncrement();	
			}
			else {
				counters.read.count.getAndIncrement();		
			}
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
