package com.aerospike.benchmarks;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Txn;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Util;

public final class MRTInsertTaskSync implements Runnable {
	private final IAerospikeClient client;
	private final Arguments args;
	private final WritePolicy writePolicy;
	private final CounterStore counters;
	private final long nMRTs;
	private final long nKeys;

	public MRTInsertTaskSync(IAerospikeClient client, Arguments args, CounterStore counters, long start, long nMRTs, long nKeys) {
		this.client = client;
		this.counters = counters;
		this.nMRTs = nMRTs;
		this.args = args;
		this.writePolicy = new WritePolicy(args.writePolicy);
		this.nKeys = nKeys;
	}

	public void run() {
		RandomShift random = new RandomShift();

		for (long i = 0; i < nMRTs; i++) {
			Txn txn = new Txn();
			writePolicy.txn = txn;

			try {
				for (long j = 0; j < nKeys; j++) {
					runCommand(random);

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
				client.commit(txn);
			} catch (Exception e) {
				System.err.println("Transaction failed for MRT iteration: " + (i+1) + " - " + e.getMessage());
				client.abort(txn);
			}
		}
	}

	private void runCommand(RandomShift random) {
		long keyRandom = random.nextLong();
		Key key = new Key(args.namespace, args.setName, keyRandom);
		// Use predictable value for 0th bin same as key value
		Bin[] bins = args.getBins(random, true, keyRandom);
		put(key, bins);

	}

	private void put(Key key, Bin[] bins) {
		if (counters.write.latency != null) {
			long begin = System.nanoTime();

			if (!skipKey(key)) {
				client.put(writePolicy, key, bins);
			}

			long elapsed = System.nanoTime() - begin;
			counters.write.count.getAndIncrement();
			counters.write.latency.add(elapsed);
		} else {
			if (!skipKey(key)) {
				client.put(writePolicy, key, bins);
			}
			counters.write.count.getAndIncrement();
		}
	}

	private boolean skipKey(Key key) {
		return args.partitionIds != null && !args.partitionIds.contains(Partition.getPartitionId(key.digest));
	}
}
