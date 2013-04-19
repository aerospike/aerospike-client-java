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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;

public final class InsertTaskSync extends InsertTask {

	private final AerospikeClient client; 

	public InsertTaskSync(
		AerospikeClient client, 
		String namespace,
		String setName,
		int startKey, 
		int nKeys, 
		int keySize, 
		int nBins, 
		int timeout, 
		DBObjectSpec[] spec, 
		CounterStore counters,
		boolean debug
	) {
		super(namespace, setName, startKey, nKeys, keySize, nBins, timeout, spec, counters, debug);
		this.client = client;
	}
	
	protected void put(WritePolicy policy, Key key, Bin[] bins) throws AerospikeException {
		client.put(policy, key, bins);
		counters.write.count.getAndIncrement();
	}
}
