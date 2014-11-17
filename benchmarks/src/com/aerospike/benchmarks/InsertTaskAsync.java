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
import com.aerospike.client.Value;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.WriteListener;

public final class InsertTaskAsync extends InsertTask implements WriteListener {
	
	private final AsyncClient client;
	
	public InsertTaskAsync(AsyncClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(args, counters, keyStart, keyCount);
		this.client = client;
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
			client.put(args.writePolicy, this, key, bins);
		}
	}

	@Override
	public void onSuccess(Key key) {
		counters.write.count.getAndIncrement();
	}

	@Override
	public void onFailure(AerospikeException ae) {
		writeFailure(ae);
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

	protected void largeListAdd(Key key, Value value) throws AerospikeException {
	}

	protected void largeStackPush(Key key, Value value) throws AerospikeException {
	}
}
