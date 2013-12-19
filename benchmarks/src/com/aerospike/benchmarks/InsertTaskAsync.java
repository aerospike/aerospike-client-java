/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.benchmarks;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.WritePolicy;

public final class InsertTaskAsync extends InsertTask implements WriteListener {
	
	private final AsyncClient client;
	
	public InsertTaskAsync(
		AsyncClient client, 
		String namespace,
		String setName,
		int startKey, 
		int nKeys, 
		int keySize, 
		int nBins, 
		WritePolicy policy, 
		DBObjectSpec[] spec, 
		CounterStore counters,
		boolean debug
	) {
		super(namespace, setName, startKey, nKeys, keySize, nBins, policy, spec, counters, debug);
		this.client = client;
	}
	
	protected void put(WritePolicy policy, Key key, Bin[] bins) throws AerospikeException {
		// If an error occurred, yield thread to back off throttle.
		// Fail counters are reset every second.
		if (counters.write.fail.get() > 0) {
			Thread.yield();
		}
		client.put(policy, this, key, bins);
	}

	@Override
	public void onSuccess(Key key) {
		counters.write.count.getAndIncrement();
	}

	@Override
	public void onFailure(AerospikeException ae) {
		writeFailure(ae);
	}
}
