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
import com.aerospike.client.policy.WritePolicy;

public final class InsertTaskSync extends InsertTask {

	private final AerospikeClient client; 

	public InsertTaskSync(AerospikeClient client, Arguments args, CounterStore counters, int keyStart, int keyCount) {
		super(args, counters, keyStart, keyCount);
		this.client = client;
	}
	
	protected void put(WritePolicy policy, Key key, Bin[] bins) throws AerospikeException {
		if (counters.write.latency != null) {
			long begin = System.currentTimeMillis();
			client.put(policy, key, bins);
			long elapsed = System.currentTimeMillis() - begin;
			counters.write.count.getAndIncrement();			
			counters.write.latency.add(elapsed);
		}
		else {
			client.put(policy, key, bins);
			counters.write.count.getAndIncrement();			
		}
	}
}
