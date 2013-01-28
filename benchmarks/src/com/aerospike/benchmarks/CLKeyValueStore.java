package com.aerospike.benchmarks;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class CLKeyValueStore {
	private static final int MAX_RECURSION_DEPTH = 10;
	
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
	
	public long SetValue(WritePolicy policy, String key, Bin[] bins) throws AerospikeException {
		long startTime = System.nanoTime();
		
		for (int i = 0; i < MAX_RECURSION_DEPTH; i++) {
			try {
				client.put(policy, new Key(this.namespace, this.set, key), bins);
				long endTime = System.nanoTime();
				return endTime - startTime;
			}
			catch (AerospikeException.Timeout aet) {
				// System.out.println(aet.getMessage());
			}
			
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		throw new AerospikeException.Timeout();
	}

	public long IncrementValue(WritePolicy policy, String key, Bin[] bins) throws AerospikeException {
		long startTime = System.nanoTime();
		
		for (int i = 0; i < MAX_RECURSION_DEPTH; i++) {
			try {
				client.add(policy, new Key(this.namespace, this.set, key), bins);
				long endTime = System.nanoTime();
				return endTime - startTime;
			}
			catch (AerospikeException.Timeout aet) {
				// System.out.println(aet.getMessage());
			}
			catch (AerospikeException ae) {
				if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
					this.counters.generationErrCnt.incrementAndGet();					
				}
				throw ae;
			}
			
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
		throw new AerospikeException.Timeout();
	}
	
	public ResponseObj GetValue(Policy policy, String key) {
		ResponseObj responseObj = new ResponseObj();
		long startTime = System.nanoTime();
		
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
		}
		catch (AerospikeException ae) {
			System.out.println(ae.getMessage());
		}

		long endTime = System.nanoTime();
		responseObj.td = endTime-startTime;
		return responseObj;
	}

	public ResponseObj GetSingleBin(Policy policy, String key, String bin) {
		ResponseObj responseObj = new ResponseObj();
		long startTime = System.nanoTime();
		
		try {
			Record record = client.get(policy, new Key(this.namespace, this.set, key), bin);
			if (record != null) {
				responseObj.value = record.bins.values().toArray();
				responseObj.generation = record.generation;
			}
		}
		catch (AerospikeException ae) {
			System.out.println(ae.getMessage());
		}

		long endTime = System.nanoTime();
		responseObj.td = endTime-startTime;
		return responseObj;
	}
}
