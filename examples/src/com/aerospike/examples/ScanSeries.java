/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
