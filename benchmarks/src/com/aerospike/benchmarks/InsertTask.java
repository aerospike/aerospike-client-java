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

import java.util.Random;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.WritePolicy;

public abstract class InsertTask implements Runnable {

	final String namespace;
	final String setName;
	final int startKey;
	final int nKeys;
	final int keySize;
	final int nBins;	
	final int timeout;
	final CounterStore counters;
	final DBObjectSpec[] spec;
	final boolean debug;
	
	public InsertTask(
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
		this.namespace = namespace;
		this.setName = setName;
		this.startKey = startKey;
		this.nKeys = nKeys;
		this.keySize = keySize;
		this.nBins = nBins;
		this.counters = counters;
		this.spec = spec;
		this.timeout = timeout;
		this.debug = debug;
	}

	public void run() {
		try {
			WritePolicy policy = new WritePolicy();
			policy.timeout = timeout;
			
			String key;
			Bin[] bins;			
			Random r = new Random();
			int i = this.counters.write.count.get();

			while (i < this.nKeys) {
				key	 = Utils.genKey(this.startKey+i, this.keySize);
				bins = Utils.genBins(r, this.nBins, this.spec, 0);
				
				try {				
					put(policy, new Key(this.namespace, this.setName, key), bins);
				}
				catch (AerospikeException ae) {
					counters.write.fail.getAndIncrement();
					if (ae.getResultCode() != ResultCode.TIMEOUT) {
						System.out.println(ae.getMessage());
					}
				}	
				catch (Exception e) {
					counters.write.fail.getAndIncrement();
					System.out.println(e.getMessage());
				}
				i++;
			}
		}
		catch (Exception ex) {
			System.out.println("Insert task error: " + ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	protected abstract void put(WritePolicy policy, Key key, Bin[] bins) throws AerospikeException;
}
