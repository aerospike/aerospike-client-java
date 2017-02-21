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

import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.util.RandomShift;

public final class InsertTaskAsync extends InsertTask {
	
	private final AsyncClient client;
	private final AtomicLong tranCount;
	private final long maxCommands;
	private final long keyStart;
	private final long keyCount;
	
	public InsertTaskAsync(AsyncClient client, Arguments args, CounterStore counters, long keyStart, long keyCount, long maxCommands) {
		super(args, counters);
		this.client = client;
		this.tranCount = new AtomicLong();
		
		if (maxCommands > keyCount) {
			maxCommands = keyCount;
		}
		this.maxCommands = maxCommands;
		this.keyStart = keyStart + maxCommands;
		this.keyCount = keyCount - maxCommands;
	}
	
	public void run() {
		// Seed selector threads with max commands.
		RandomShift random = RandomShift.instance();
			
		for (long i = keyStart - maxCommands; i < keyStart; i++) {
			try {
				runCommand(i, random);
			}
			catch (AerospikeException ae) {
				i--;
				writeFailure(ae);
			}	
			catch (Exception e) {
				i--;
				writeFailure(e);
			}
		}
	}

	private void runCommand(long currentKey, RandomShift random) {
		Key key = new Key(args.namespace, args.setName, currentKey);
		Bin[] bins = args.getBins(random, true, currentKey);
		
		if (counters.write.latency != null) {		
			client.put(args.writePolicy, new LatencyWriteHandler(currentKey), key, bins);
		}
		else {
			client.put(args.writePolicy, new WriteHandler(currentKey), key, bins);
		}
	}

	private void writeSuccess() {
		counters.write.count.getAndIncrement();
		
		long count = tranCount.getAndIncrement();
		
		if (count >= keyCount) {
			return;
		}
		runCommand(keyStart + count, RandomShift.instance());
	}
	
	private final class LatencyWriteHandler implements WriteListener {
		private long currentKey;
		private long begin;
		
		public LatencyWriteHandler(long currentKey) {
			this.currentKey = currentKey;
			this.begin = System.nanoTime();
		}
		
		@Override
		public void onSuccess(Key key) {
			long elapsed = System.nanoTime() - begin;
			counters.write.latency.add(elapsed);
			writeSuccess();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
			runCommand(currentKey, RandomShift.instance());
		}		
	}
	
	private final class WriteHandler implements WriteListener {
		private long currentKey;
		
		public WriteHandler(long currentKey) {
			this.currentKey = currentKey;
		}
		
		@Override
		public void onSuccess(Key key) {
			writeSuccess();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
			runCommand(currentKey, RandomShift.instance());
		}		
	}
}
