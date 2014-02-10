/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.examples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.ScanPolicy;

public class ScanSeries extends Example implements ScanCallback {

	private Map<String,Metrics> setMap = new HashMap<String,Metrics>();

	public ScanSeries(Console console) {
		super(console);
	}

	/**
	 * Scan all nodes in series and read all records in all sets.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		console.info("Scan series: namespace=" + params.namespace + " set=" + params.set);
		
		// Use low scan priority.  This will take more time, but it will reduce
		// the load on the server.
		ScanPolicy policy = new ScanPolicy();
		policy.maxRetries = 1;
		policy.priority = Priority.LOW;
		
		List<String> nodeList = client.getNodeNames();
		long begin = System.currentTimeMillis();
		
		for (String nodeName : nodeList) {
			console.info("Scan node " + nodeName);
			client.scanNode(policy, nodeName, params.namespace, params.set, this);
			
			for (Map.Entry<String,Metrics> entry : setMap.entrySet()) {
				console.info("Node " + nodeName + " set " + entry.getKey() + " count: " +  entry.getValue().count);
				entry.getValue().count = 0;
			}
		}

		long end = System.currentTimeMillis();
		double seconds =  (double)(end - begin) / 1000.0;
		console.info("Elapsed time: " + seconds + " seconds");
		
		long total = 0;
		
		for (Map.Entry<String,Metrics> entry : setMap.entrySet()) {
			console.info("Total set " + entry.getKey() + " count: " +  entry.getValue().total);
			total += entry.getValue().total;
		}
		console.info("Grand total: " + total);
		double performance = Math.round((double)total / seconds);
		console.info("Records/second: " + performance);
	}

	@Override
	public void scanCallback(Key key, Record record) {
		Metrics metrics = setMap.get(key.setName);
		
		if (metrics == null) {
			metrics = new Metrics();
		}
		metrics.count++;
		metrics.total++;
		setMap.put(key.setName, metrics);
		
		/*
		System.out.print(key.namespace + ',' + key.setName + ',' + Buffer.bytesToHexString(key.digest));
		
		if (record.bins != null) {
			for (Entry<String,Object> entry : record.bins.entrySet()) {	
				System.out.print(',');
				System.out.print(entry.getKey());
				System.out.print(',');
				System.out.print(entry.getValue());
			}
		}
		System.out.println();
		*/
	}
	
	private static class Metrics {
		public long count = 0;
		public long total = 0;
	}
}
