package com.aerospike.benchmarks;

import java.util.Random;

import com.aerospike.client.Bin;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;

public class InsertTask implements Runnable {

	int startKey;
	int nKeys;
	int keySize;
	int nBins;	
	int timeout;
	CounterStore counters;
	CLKeyValueStore kvs;
	DBObjectSpec[] spec;
	
	public InsertTask(CLKeyValueStore kvs, int startKey, int nKeys, int keySize, int nBins, int timeout, DBObjectSpec[] spec, CounterStore counters) {
		this.kvs = kvs;
		this.startKey = startKey;
		this.nKeys    = nKeys;
		this.keySize  = keySize;
		this.nBins    = nBins;
		this.counters = counters;
		this.spec      = spec;
		this.timeout  = timeout;
	}

	public void run() {
		try {
			WritePolicy policy = new WritePolicy();
			policy.timeout = timeout;
			
			String key;
			Bin[] bins;			
			Random r = new Random();
			int i = this.counters.wcounter.getAndIncrement();

			while (i < this.nKeys) {
				key	 = Utils.genKey(this.startKey+i, this.keySize);
				bins = Utils.genBins(r, this.nBins, this.spec, 0);
				
				try {
					this.kvs.SetValue(policy, key, bins);
				}
				catch (Exception e) {
					System.out.println(e.getMessage());
					Util.sleep(10);
				}
				i = this.counters.wcounter.getAndIncrement();
			}
		}
		catch (Exception ex) {
			System.out.println("Insert error: " + ex.getMessage());
			ex.printStackTrace();
		}
	}
}
