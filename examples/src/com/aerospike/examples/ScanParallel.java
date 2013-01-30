package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;

public class ScanParallel extends Example implements ScanCallback {

	private int recordCount = 0;

	public ScanParallel(Console console) {
		super(console);
	}

	/**
	 * Scan all nodes in parallel and read all records in a set.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		console.info("Scan database.");
		recordCount = 0;
		long begin = System.currentTimeMillis();
		ScanPolicy policy = new ScanPolicy();
		client.scanAll(policy, params.namespace, params.set, this);

		long end = System.currentTimeMillis();
		double seconds =  (double)(end - begin) / 1000.0;
		console.info("Total records returned: " + recordCount);
		console.info("Elapsed time: " + seconds + " seconds");
		double performance = Math.round((double)recordCount / seconds);
		console.info("Records/second: " + performance);
	}

	@Override
	public void scanCallback(Key key, Record record) {
		recordCount++;

		if ((recordCount % 10000) == 0) {
			console.info("Records " + recordCount);
		}
	}
}
