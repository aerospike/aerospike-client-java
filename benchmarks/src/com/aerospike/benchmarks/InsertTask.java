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

import java.util.Random;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;

public abstract class InsertTask implements Runnable {

	final Arguments args;
	final int keyStart;
	final int keyCount;
	final CounterStore counters;
	
	public InsertTask(Arguments args, CounterStore counters, int keyStart, int keyCount) {
		this.args = args;
		this.counters = counters;
		this.keyStart = keyStart;
		this.keyCount = keyCount;
	}

	public void run() {
		try {			
			Random random = new Random();

			for (int i = 0; i < keyCount; i++) {
				try {
					Key key = new Key(args.namespace, args.setName, keyStart + i);
					Bin[] bins = args.getBins(random, true);
					
					switch (args.storeType) {
					case KVS:
						put(key, bins);
						break;
						
					case LLIST:
						largeListAdd(key, bins[0].value);
						break;

					case LSTACK:
						largeStackPush(key, bins[0].value);
						break;
					}
				}
				catch (AerospikeException ae) {
					writeFailure(ae);
				}	
				catch (Exception e) {
					writeFailure(e);
				}
			}
		}
		catch (Exception ex) {
			System.out.println("Insert task error: " + ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	protected void writeFailure(AerospikeException ae) {
		if (ae.getResultCode() == ResultCode.TIMEOUT) {		
			counters.write.timeouts.getAndIncrement();
		}
		else {			
			counters.write.errors.getAndIncrement();
			
			if (args.debug) {
				ae.printStackTrace();
			}
		}
	}

	protected void writeFailure(Exception e) {
		counters.write.errors.getAndIncrement();
		
		if (args.debug) {
			e.printStackTrace();
		}
	}
	
	protected abstract void put(Key key, Bin[] bins) throws AerospikeException;
	protected abstract void largeListAdd(Key key, Value value) throws AerospikeException;
	protected abstract void largeStackPush(Key key, Value value) throws AerospikeException;
}
