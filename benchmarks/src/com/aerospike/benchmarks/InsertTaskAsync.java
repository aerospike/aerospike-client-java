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
package com.aerospike.benchmarks;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.util.RandomShift;

public final class InsertTaskAsync extends InsertTask {

	private final AerospikeClient client;
	private final EventLoop eventLoop;
	private final RandomShift random;
	private final WriteListener listener;
	private final long keyStart;
	private final long keyMax;
	private long keyCount;
	private long begin;
	private final boolean useLatency;

	public InsertTaskAsync(
		AerospikeClient client,
		EventLoop eventLoop,
		Arguments args,
		CounterStore counters,
		long keyStart,
		long keyMax
	) {
		super(args, counters);
		this.client = client;
		this.eventLoop = eventLoop;
		this.random = new RandomShift();
		this.keyStart = keyStart;
		this.keyMax = keyMax;
		this.useLatency = counters.write.latency != null;

		if (useLatency) {
			listener = new LatencyWriteHandler();
		}
		else {
			listener = new WriteHandler();
		}
	}

	public void runCommand() {
		long currentKey = keyStart + keyCount;
		Key key = new Key(args.namespace, args.setName, currentKey);
		Bin[] bins = args.getBins(random, true, currentKey);

		if (useLatency) {
			begin = System.nanoTime();
		}
		client.put(eventLoop, listener, args.writePolicy, key, bins);
	}

	private final class LatencyWriteHandler implements WriteListener {
		@Override
		public void onSuccess(Key key) {
			long elapsed = System.nanoTime() - begin;
			counters.write.latency.add(elapsed);
			counters.write.count.getAndIncrement();
			keyCount++;

			if (keyCount < keyMax) {
				// Try next command.
				runCommand();
			}
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
			// Retry command with same key.
			runCommand();
		}
	}

	private final class WriteHandler implements WriteListener {
		@Override
		public void onSuccess(Key key) {
			counters.write.count.getAndIncrement();
			keyCount++;

			if (keyCount < keyMax) {
				// Try next command.
				runCommand();
			}
		}

		@Override
		public void onFailure(AerospikeException ae) {
			writeFailure(ae);
			// Retry command with same key.
			runCommand();
		}
	}
}
