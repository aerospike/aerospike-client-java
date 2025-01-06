/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Txn;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Util;

public final class MRTInsertTaskSync extends MRTInsertTask implements Runnable {
	private final IAerospikeClient client;
	private final long keyStart;
	private final long keysPerMRT;
	private final long nMRTs;

	public MRTInsertTaskSync(IAerospikeClient client, Arguments args, CounterStore counters, long keyStart,
			long keysPerMRT, long nMRTs) {
		super(args, counters);
		this.client = client;
		this.keyStart = keyStart;
		this.keysPerMRT = keysPerMRT;
		this.nMRTs = nMRTs;
	}

	public void run() {
		RandomShift random = new RandomShift();
		long begin;
		long uowElapse;
		boolean uowCompleted;

		for (long i = 0; i < nMRTs; i++) {
			Txn txn = new Txn();
			txn.setTimeout(txnTimeoutSeconds);
			writePolicy.txn = txn;
			begin = System.nanoTime();
			uowElapse = 0;
			uowCompleted = false;

			long startKey = keyStart + keysPerMRT * i;

			try {
				for (long j = 0; j < keysPerMRT; j++) {
					try {
						runCommand(startKey + j, random);
					} catch (AerospikeException ae) {
						i--;
						writeFailure(ae);
					} catch (Exception e) {
						i--;
						writeFailure(e);
					}

					// Throttle throughput
					if (args.throughput > 0) {
						int transactions = counters.write.count.get();

						if (transactions > args.throughput) {
							long millis = counters.periodBegin.get() + 1000L - System.currentTimeMillis();

							if (millis > 0) {
								Util.sleep(millis);
							}
						}
					}
				}

				if(Main.abortRun.get()) {
					PerformMRTAbort(client, txn);
					break;
				}

				MRTHandleResult result = CompleteUoW(client, txn, begin, true);
				uowCompleted = result.successful;
				uowElapse+= result.totalElapseTime;

				if(Main.terminateRun.get()) {
					break;
				}

			} catch (Exception e) {
				MRTHandleResult result = CompleteUoW(client, txn, begin, false);
				uowCompleted = result.successful;
				uowElapse += result.totalElapseTime;
			}
			if(uowCompleted && uowElapse > 0) {
				counters.mrtUnitOfWork.recordElapsedTimeOTel(LatencyTypes.MRTUOWTOTAL, uowElapse);
			}

			if(Main.abortRun.get() || Main.terminateRun.get()) {
				break;
			}
		}
	}

	private void runCommand(long keyCurrent, RandomShift random) {
		Key key = new Key(args.namespace, args.setName, keyCurrent);
		// Use predictable value for 0th bin same as key value
		Bin[] bins = args.getBins(random, true, keyCurrent);
		put(key, bins);
	}

	private void put(Key key, Bin[] bins) {

		if (skipKey(key)) {
			counters.write.count.getAndIncrement();
		}

		putUoW(client, this.writePolicy, key, bins);
	}

	private boolean skipKey(Key key) {
		return args.partitionIds != null && !args.partitionIds.contains(Partition.getPartitionId(key.digest));
	}
}
