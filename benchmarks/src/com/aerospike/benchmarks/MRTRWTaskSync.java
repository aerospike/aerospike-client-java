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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Util;

/**
 * Synchronous read/write task.
 */
public class MRTRWTaskSync extends MRTRWTask implements Runnable {

	private final IAerospikeClient client;

	private final long nMRTs;
	private final long keysPerMRT;

	public MRTRWTaskSync(IAerospikeClient client, Arguments args, CounterStore counters, long nMRTs, long keyStart,
						 long keyCount, long keysPerMRT) {
		super(args, counters, keyStart, keyCount);

		this.client = client;
		this.nMRTs = nMRTs;
		this.keysPerMRT = keysPerMRT;
	}

	public void run() {
		RandomShift random = new RandomShift();
		long begin;
		long uowElapse;
		boolean uowCompleted;

		//uow (Unit of Work) consist of the actions (get/puts) within a MRT.
		while (valid) {
			for (long i = 0; i < nMRTs; i++) {
				Txn txn = new Txn();
				txn.setTimeout(txnTimeoutSeconds);
				writePolicy.txn = txn;
				readPolicy.txn = txn;
				updatePolicy.txn = txn;
				replacePolicy.txn = txn;
				batchPolicy.txn = txn;
				writePolicyGeneration.txn = txn;

				begin = System.nanoTime();
				uowElapse = 0;
				uowCompleted = false;

				if(Main.abortRun.get() || Main.terminateRun.get()) {
					break;
				}

				try {
					for (int k = 0; k < keysPerMRT; k++) {
						runCommand(random);
						if(Main.abortRun.get() || Main.terminateRun.get()) {
							break;
						}
						// Throttle throughput
						if (args.throughput > 0) {
							int transactions;
							if (counters.transaction.latency != null) {
								// Measure the transactions as per one "business" transaction
								transactions = counters.transaction.count.get();
							} else {
								transactions = counters.write.count.get() + counters.read.count.get();
							}
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

					MRTHandleResult result = CompleteUoW(client, txn, begin, valid);
					uowCompleted = result.successful;
					uowElapse += result.totalElapseTime;

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
			}

			if(Main.abortRun.get() || Main.terminateRun.get()) {
				break;
			}
		}
	}

	@Override
	protected void put(WritePolicy writePolicy, Key key, Bin[] bins) {
		if (skipKey(key)) {
			counters.write.count.getAndIncrement();
			return;
		}
		if(writePolicy == null) {
			writePolicy = this.writePolicy;
		}

		putUoW(client, this.writePolicy, key, bins);
	}

	@Override
	protected void add(Key key, Bin[] bins) {
		if (skipKey(key)) {
			counters.write.count.getAndIncrement();
			return;
		}

		addUoW(client, writePolicyGeneration, key, bins);
	}

	@Override
	protected void get(Key key, String binName) {
		if (skipKey(key)) {
			processRead(key, new Object());
			return;
		}

		processRead(key, getUoW(client, writePolicyGeneration, key, binName));
	}

	@Override
	protected void get(Key key) {
		if (skipKey(key)) {
			processRead(key, new Object());
			return;
		}

		processRead(key, getUoW(client, writePolicyGeneration, key));
	}

	@Override
	protected void get(Key key, String udfPackageName, String udfFunctionName, Value[] udfValues) {
		if (skipKey(key)) {
			processRead(key, new Object());
			return;
		}

		Object udfReturnObj;

		if (counters.read.latency != null) {
			long begin = System.nanoTime();

			udfReturnObj = client.execute(this.writePolicy, key, udfPackageName, udfFunctionName, udfValues);
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
		} else {
			udfReturnObj = client.execute(this.writePolicy, key, udfPackageName, udfFunctionName, udfValues);
		}
		processRead(key, udfReturnObj);
	}

	@Override
	protected void get(Key[] keys, String binName) {
		if (args.partitionIds != null) {
			keys = getFilteredKeys(keys);
		}

		final Record[] records = getUoW(client, args.batchPolicy, keys, binName);

		if (records == null) {
			System.out.println("Batch records returned is null");
		}
		//processBatchRead();
	}

	@Override
	protected void get(Key[] keys) {
		if (args.partitionIds != null) {
			keys = getFilteredKeys(keys);
		}

		final Record[] records = getUoW(client, args.batchPolicy, keys);

		if (records == null) {
			System.out.println("Batch records returned is null");
		}
		//processBatchRead();
	}

	private boolean skipKey(Key key) {
		return args.partitionIds != null && !args.partitionIds.contains(Partition.getPartitionId(key.digest));
	}

	private Key[] getFilteredKeys(Key[] keys) {
		List<Key> filteredKeys = new ArrayList<>();

		for (Key key : keys) {
			if (!skipKey(key)) {
				filteredKeys.add(key);
			}
		}

		return filteredKeys.toArray(new Key[0]);
	}
}
