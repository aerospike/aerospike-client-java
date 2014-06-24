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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class QueryString extends Example {

	public QueryString(Console console) {
		super(console);
	}

	/**
	 * Create secondary index on a string bin and query on it.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Query functions are not supported by the connected Aerospike server.");
			return;
		}
		
		String indexName = "queryindex";
		String keyPrefix = "querykey";
		String valuePrefix = "queryvalue";
		String binName = params.getBinName("querybin");  
		int size = 5;

		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, binName, valuePrefix, size);
		runQuery(client, params, indexName, binName, valuePrefix);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);		
	}
	
	private void createIndex(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			params.namespace, params.set, indexName, binName);			
		
		Policy policy = new Policy();
		policy.timeout = 0; // Do not timeout on index create.
		IndexTask task = client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.STRING);
		task.waitTillComplete();
	}

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		String valuePrefix,
		int size
	) throws Exception {
		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, valuePrefix + i);
			
			console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, bin.value);
			
			client.put(params.writePolicy, key, bin);
		}
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName,
		String valuePrefix
	) throws Exception {
		
		String filter = valuePrefix + 3;
		
		console.info("Query for: ns=%s set=%s index=%s bin=%s filter=%s",
			params.namespace, params.set, indexName, binName, filter);			
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(binName);
		stmt.setFilters(Filter.equal(binName, filter));
		
		RecordSet rs = client.query(null, stmt);
		
		try {
			int count = 0;
			
			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				String result = (String)record.getValue(binName);
				
				if (result.equals(filter)) {
					console.info("Record found: ns=%s set=%s bin=%s key=%s value=%s",
						key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);
				}
				else {
					console.error("Query mismatch: Expected %s. Received %s.", filter, result);
				}
				count++;
			}
			
			if (count == 0) {
				console.error("Query failed. No records returned.");			
			}
		}
		finally {
			rs.close();
		}
	}
}
