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
package com.aerospike.benchmarks;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;

public final class RWTaskAsync extends RWTask {
	
	private final AsyncClient client;
	private final WriteHandler writeHandler;
	private final ReadHandler readHandler;
	private final BatchReadHandler batchReadHandler;
	private final long maxCommands;

	public RWTaskAsync(AsyncClient client, Arguments args, CounterStore counters, long keyStart, long keyCount, long maxCommands) {
		super(args, counters, keyStart, keyCount);
		this.client = client;
		
		if (maxCommands > keyCount) {
			maxCommands = keyCount;
		}
		this.maxCommands = maxCommands;
		writeHandler = new WriteHandler();
		readHandler = new ReadHandler();
		batchReadHandler = new BatchReadHandler();
	}
	
	public void run() {
		// Seed selector threads with max commands.
		RandomShift random = RandomShift.instance();
				
		for (int i = 0; i < maxCommands; i++) {
			runCommand(random);
		}		
	}

	@Override
	protected void runNextCommand() {
		if (valid) {
			runCommand(RandomShift.instance());
		}
	}

	@Override
	protected void put(WritePolicy policy, Key key, Bin[] bins) {
		if (counters.write.latency != null) {
			client.put(policy, new LatencyWriteHandler(), key, bins);	
		}
		else {
			client.put(policy, writeHandler, key, bins);
		}
	}
	
	@Override
	protected void add(Key key, Bin[] bins) {		
		if (counters.write.latency != null) {
			client.add(writePolicyGeneration, new LatencyWriteHandler(), key, bins);
		}
		else {
			client.add(writePolicyGeneration, writeHandler, key, bins);			
		}
	}

	@Override
	protected void get(Key key, String binName) {		
		if (counters.read.latency != null) {		
			client.get(args.readPolicy, new LatencyReadHandler(), key, binName);
		}
		else {			
			client.get(args.readPolicy, readHandler, key, binName);
		}
	}

	@Override
	protected void get(Key key) throws AerospikeException {
		if (counters.read.latency != null) {	
			client.get(args.readPolicy, new LatencyReadHandler(), key);
		}
		else {			
			client.get(args.readPolicy, readHandler, key);
		}
	}

	@Override
	protected void get(Key[] keys, String binName) throws AerospikeException {
		if (counters.read.latency != null) {	
			client.get(args.batchPolicy, new LatencyBatchReadHandler(), keys, binName);
		}
		else {
			client.get(args.batchPolicy, batchReadHandler, keys, binName);
		}
	}

	@Override
	protected void get(Key[] keys) throws AerospikeException {
		if (counters.read.latency != null) {	
			client.get(args.batchPolicy, new LatencyBatchReadHandler(), keys);
		}
		else {
			client.get(args.batchPolicy, batchReadHandler, keys);
		}
	}

	private final class WriteHandler implements WriteListener {	
		@Override
		public void onSuccess(Key key) {
			counters.write.count.getAndIncrement();
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
			runNextCommand();
		}		
	}

	private final class LatencyWriteHandler implements WriteListener {
		private long begin;
		
		public LatencyWriteHandler() {
			this.begin = System.nanoTime();
		}
		
		@Override
		public void onSuccess(Key key) {
			long elapsed = System.nanoTime() - begin;
			counters.write.latency.add(elapsed);
			counters.write.count.getAndIncrement();
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
			runNextCommand();
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
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
			runNextCommand();
		}
	}
	
	private final class LatencyReadHandler implements RecordListener {
		private long begin;
		
		public LatencyReadHandler() {
			this.begin = System.nanoTime();
		}
		
		@Override
		public void onSuccess(Key key, Record record) {
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
			
			if (record == null && args.reportNotFound) {
				counters.readNotFound.getAndIncrement();	
			}
			else {
				counters.read.count.getAndIncrement();		
			}
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
			runNextCommand();
		}		
	}
	
	private final class BatchReadHandler implements RecordArrayListener {
		@Override
		public void onSuccess(Key[] keys, Record[] records) {
			for (int i = 0; i < records.length; i++) {
				if (records[i] == null && args.reportNotFound) {
					counters.readNotFound.getAndIncrement();	
				}
				else {
					counters.read.count.getAndIncrement();		
				}
			}
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
			runNextCommand();
		}
	}
	
	private final class LatencyBatchReadHandler implements RecordArrayListener {
		private long begin;
		
		public LatencyBatchReadHandler() {
			this.begin = System.nanoTime();
		}
		
		@Override
		public void onSuccess(Key[] keys, Record[] records) {
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
			
			for (int i = 0; i < records.length; i++) {
				if (records[i] == null && args.reportNotFound) {
					counters.readNotFound.getAndIncrement();	
				}
				else {
					counters.read.count.getAndIncrement();		
				}
			}
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
			runNextCommand();
		}		
	}
	
	protected void largeListAdd(Key key, Value value) {
	}

	protected void largeListGet(Key key) {
	}

	protected void largeStackPush(Key key, Value value) {
	}

	protected void largeStackPeek(Key key) {
	}
}
