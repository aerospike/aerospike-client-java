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
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;

public final class RWTaskAsync extends RWTask {

	private final AerospikeClient client;
	private final EventLoop eventLoop;
	private final RandomShift random;
	private final WriteListener writeListener;
	private final RecordListener recordListener;
	private final RecordArrayListener recordArrayListener;
	private long begin;
	private final boolean useLatency;

	public RWTaskAsync(
		AerospikeClient client,
		EventLoop eventLoop,
		Arguments args,
		CounterStore counters,
		long keyStart,
		long keyCount
	) {
		super(args, counters, keyStart, keyCount);
		this.client = client;
		this.eventLoop = eventLoop;
		this.random = new RandomShift();
		this.useLatency = counters.write.latency != null;

		if (useLatency) {
			writeListener = new LatencyWriteHandler();
			recordListener = new LatencyReadHandler();
			recordArrayListener = new LatencyBatchReadHandler();
		}
		else {
			writeListener = new WriteHandler();
			recordListener = new ReadHandler();
			recordArrayListener = new BatchReadHandler();
		}
	}

	@Override
	protected void runNextCommand() {
		if (valid) {
			runCommand(random);
		}
	}

	@Override
	protected void put(WritePolicy policy, Key key, Bin[] bins) {
		if (useLatency) {
			begin = System.nanoTime();
		}
		client.put(eventLoop, writeListener, policy, key, bins);
	}

	@Override
	protected void add(Key key, Bin[] bins) {
		if (useLatency) {
			begin = System.nanoTime();
		}
		client.add(eventLoop, writeListener, writePolicyGeneration, key, bins);
	}

	@Override
	protected void get(Key key, String binName) {
		if (useLatency) {
			begin = System.nanoTime();
		}
		client.get(eventLoop, recordListener, args.readPolicy, key, binName);
	}

	@Override
	protected void get(Key key) throws AerospikeException {
		if (useLatency) {
			begin = System.nanoTime();
		}
		client.get(eventLoop, recordListener, args.readPolicy, key);
	}

	@Override
	protected void get(Key key, String udfPackageName, String udfFunctionName, Value[] udfValues) {
		// Udfs for async is not implemented
	}

	@Override
	protected void get(Key[] keys, String binName) throws AerospikeException {
		if (useLatency) {
			begin = System.nanoTime();
		}
		client.get(eventLoop, recordArrayListener, args.batchPolicy, keys, binName);
	}

	@Override
	protected void get(Key[] keys) throws AerospikeException {
		if (useLatency) {
			begin = System.nanoTime();
		}
		client.get(eventLoop, recordArrayListener, args.batchPolicy, keys);
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
			counters.read.count.getAndIncrement();
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
			runNextCommand();
		}
	}

	private final class LatencyBatchReadHandler implements RecordArrayListener {
		@Override
		public void onSuccess(Key[] keys, Record[] records) {
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
			counters.read.count.getAndIncrement();
			runNextCommand();
		}

		@Override
		public void onFailure(AerospikeException ae) {
			readFailure(ae);
			runNextCommand();
		}
	}
}
