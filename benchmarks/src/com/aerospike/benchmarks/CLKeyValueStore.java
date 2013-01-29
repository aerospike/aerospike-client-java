package com.aerospike.benchmarks;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class CLKeyValueStore {
	
	AerospikeClient client = null;
	String namespace = null;
	String set = null;
	CounterStore counters;
	
	public CLKeyValueStore(String hostname, int port, String namespace, String set, CounterStore counters) 
		throws AerospikeException {
		this.namespace = namespace;
        this.set = set;        
		this.counters = counters;

		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = true;	
		client = new AerospikeClient(policy, hostname, port);		
	}
	
	public void SetValue(WritePolicy policy, String key, Bin[] bins) throws AerospikeException {
		client.put(policy, new Key(this.namespace, this.set, key), bins);
	}

	public void IncrementValue(WritePolicy policy, String key, Bin[] bins) throws AerospikeException {
		client.add(policy, new Key(this.namespace, this.set, key), bins);
	}
	
	public ResponseObj GetValue(Policy policy, String key) {
		ResponseObj responseObj = new ResponseObj();
		
		try {
			Record record = client.get(policy, new Key(this.namespace, this.set, key));
			
			if (record != null) {
				Map<String,Object> bins = record.bins;
				int max = bins.size();
				Object[] objarr = new Object[max];
				
				for (int i = 0; i < max; i++) {
					objarr[i] = bins.get(Integer.toString(i));
				}
				responseObj.value = objarr;
				responseObj.generation = record.generation;
			}
			counters.read.count.getAndIncrement();
		}
		catch (Exception e) {
			counters.read.fail.getAndIncrement();
			System.out.println(e.getMessage());
		}
		return responseObj;
	}

	public ResponseObj GetSingleBin(Policy policy, String key, String bin) {
		ResponseObj responseObj = new ResponseObj();
		
		try {
			Record record = client.get(policy, new Key(this.namespace, this.set, key), bin);
			if (record != null) {
				responseObj.value = record.bins.values().toArray();
				responseObj.generation = record.generation;
			}
			counters.read.count.getAndIncrement();
		}
		catch (Exception e) {
			counters.read.fail.getAndIncrement();
			System.out.println(e.getMessage());
		}
		return responseObj;
	}
}
