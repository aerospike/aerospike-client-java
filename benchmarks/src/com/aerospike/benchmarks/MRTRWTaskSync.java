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
import com.aerospike.client.policy.Policy;
import com.aerospike.client.AerospikeException;

/**
 * Synchronous read/write task.
 */
public class MRTRWTaskSync extends MRTRWTask implements Runnable {

	private final IAerospikeClient client;
	private final WritePolicy writePolicy;
	private final Policy readPolicy;
	private final long nMRTs;
	private final long keysPerMRT;

	public MRTRWTaskSync(IAerospikeClient client, Arguments args, CounterStore counters, long nMRTs, long keyStart,
						 long keyCount, long keysPerMRT) {
		super(args, counters, keyStart, keyCount);
		this.writePolicy = new WritePolicy(args.writePolicy);
		this.readPolicy = new Policy(args.readPolicy);
		this.client = client;
		this.nMRTs = nMRTs;
		this.keysPerMRT = keysPerMRT;
	}

	public void run() {
		RandomShift random = new RandomShift();
		long begin;
		long uowElapse;
		boolean withinCommit;
		boolean withinAbort;
		boolean uowCompleted;

		//uow (Unit of Work) consist of the actions (get/puts) within a MRT.
		while (valid) {
			for (long i = 0; i < nMRTs; i++) {
				withinCommit = false;
				withinAbort = false;
				uowCompleted = false;
				begin = System.nanoTime();
				uowElapse = 0;
				Txn txn = new Txn();
				writePolicy.txn = txn;
				readPolicy.txn = txn;

				try {
					for (int k = 0; k < keysPerMRT; k++) {
						runCommand(random);
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

					if(counters.mrtUnitOfWork.latency != null) {
						uowElapse = System.nanoTime() - begin;
						counters.mrtUnitOfWork.count.getAndIncrement();
						counters.mrtUnitOfWork.latency.add(uowElapse);
					}
					else {
						counters.mrtUnitOfWork.count.getAndIncrement();
						counters.mrtUnitOfWork.incrTransCountOTel(LatencyTypes.MRTUOW);
					}

					if (valid) {
						begin = System.nanoTime();
						withinCommit = true;
						client.commit(txn);
						if(counters.mrtCommit.latency != null) {
							long elapsed = System.nanoTime() - begin;
							uowElapse += elapsed;
							counters.mrtCommit.count.getAndIncrement();
							counters.mrtCommit.latency.add(elapsed);
						}
						else {
							counters.mrtCommit.count.getAndIncrement();
							counters.mrtCommit.incrTransCountOTel(LatencyTypes.MRTCOMMIT);
						}
					} else {
						begin = System.nanoTime();
						withinAbort = true;
						client.abort(txn);
						if(counters.mrtAbort.latency != null) {
							long elapsed = System.nanoTime() - begin;
							uowElapse += elapsed;
							counters.mrtAbort.count.getAndIncrement();
							counters.mrtAbort.latency.add(elapsed);
						}
						else {
							counters.mrtAbort.count.getAndIncrement();
							counters.mrtAbort.incrTransCountOTel(LatencyTypes.MRTABORT);
						}
					}
					uowCompleted = true;
				} catch (AerospikeException e) {
					if(withinAbort) {
						counters.mrtAbort.errors.incrementAndGet();
						counters.mrtAbort.addExceptionOTel(e, LatencyTypes.MRTABORT);
					} else if (withinCommit) {
						counters.mrtCommit.errors.incrementAndGet();
						counters.mrtCommit.addExceptionOTel(e, LatencyTypes.MRTCOMMIT);
					}
					begin = System.nanoTime();
					client.abort(txn);
					if(counters.mrtAbort.latency != null) {
						long elapsed = System.nanoTime() - begin;
						uowElapse += elapsed;
						counters.mrtAbort.count.getAndIncrement();
						counters.mrtAbort.latency.add(elapsed);
					}
					else {
						counters.mrtAbort.count.getAndIncrement();
						counters.mrtAbort.incrTransCountOTel(LatencyTypes.MRTABORT);
					}
				}
				if(uowCompleted) {
					counters.mrtUnitOfWork.recordElapsedTimeOTel(LatencyTypes.MRTUOWTOTAL, uowElapse);
				}
			}
		}
	}

	@Override
	protected void put(WritePolicy writePolicy, Key key, Bin[] bins) {
		if (skipKey(key)) {
			counters.write.count.getAndIncrement();
			return;
		}

		if (counters.write.latency != null) {
			long begin = System.nanoTime();
			client.put(this.writePolicy, key, bins);
			long elapsed = System.nanoTime() - begin;
			counters.write.count.getAndIncrement();
			counters.write.latency.add(elapsed);
		} else {
			client.put(this.writePolicy, key, bins);
			counters.write.count.getAndIncrement();
			counters.write.incrTransCountOTel(LatencyTypes.WRITE);
		}
	}

	@Override
	protected void add(Key key, Bin[] bins) {
		if (skipKey(key)) {
			counters.write.count.getAndIncrement();
			return;
		}

		if (counters.write.latency != null) {
			long begin = System.nanoTime();
			client.add(writePolicyGeneration, key, bins);
			long elapsed = System.nanoTime() - begin;
			counters.write.count.getAndIncrement();
			counters.write.latency.add(elapsed);
		} else {
			client.add(writePolicyGeneration, key, bins);
			counters.write.count.getAndIncrement();
			counters.write.incrTransCountOTel(LatencyTypes.WRITE);
		}
	}

	@Override
	protected void get(Key key, String binName) {
		if (skipKey(key)) {
			processRead(key, new Object());
			return;
		}

		Record record;

		if (counters.read.latency != null) {
			long begin = System.nanoTime();
			record = client.get(this.readPolicy, key, binName);
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
		} else {
			record = client.get(this.readPolicy, key, binName);
			counters.read.incrTransCountOTel(LatencyTypes.READ);
		}
		processRead(key, record);
	}

	@Override
	protected void get(Key key) {
		if (skipKey(key)) {
			processRead(key, new Object());
			return;
		}

		Record record;

		if (counters.read.latency != null) {
			long begin = System.nanoTime();
			record = client.get(this.readPolicy, key);
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
		} else {
			record = client.get(this.readPolicy, key);
			counters.read.incrTransCountOTel(LatencyTypes.READ);
		}
		processRead(key, record);
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

		Record[] records;

		if (counters.read.latency != null) {
			long begin = System.nanoTime();
			records = client.get(args.batchPolicy, keys, binName);
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
		} else {
			records = client.get(args.batchPolicy, keys, binName);
			counters.read.incrTransCountOTel(LatencyTypes.READ);
		}

		if (records == null) {
			System.out.println("Batch records returned is null");
		}
		processBatchRead();
	}

	@Override
	protected void get(Key[] keys) {
		if (args.partitionIds != null) {
			keys = getFilteredKeys(keys);
		}

		Record[] records;

		if (counters.read.latency != null) {
			long begin = System.nanoTime();
			records = client.get(args.batchPolicy, keys);
			long elapsed = System.nanoTime() - begin;
			counters.read.latency.add(elapsed);
		} else {
			records = client.get(args.batchPolicy, keys);
			counters.read.incrTransCountOTel(LatencyTypes.READ);
		}

		if (records == null) {
			System.out.println("Batch records returned is null");
		}
		processBatchRead();
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
