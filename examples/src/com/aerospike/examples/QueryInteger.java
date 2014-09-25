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

public class QueryInteger extends Example {

	public QueryInteger(Console console) {
		super(console);
	}

	/**
	 * Create secondary index on an integer bin and query on it.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Query functions are not supported by the connected Aerospike server.");
			return;
		}
		
		String indexName = "queryindexint";
		String keyPrefix = "querykeyint";
		String binName = params.getBinName("querybinint");  
		int size = 50;

		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, binName, size);
		runQuery(client, params, indexName, binName);
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
		IndexTask task = client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.NUMERIC);
		task.waitTillComplete();
	}

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		int size
	) throws Exception {
		console.info("Write " + size + " records.");

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);	
			client.put(params.writePolicy, key, bin);
		}
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		
		int begin = 14;
		int end = 18;
		
		console.info("Query for: ns=%s set=%s index=%s bin=%s >= %s <= %s",
			params.namespace, params.set, indexName, binName, begin, end);			
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(binName);
		stmt.setFilters(Filter.range(binName, begin, end));
		
		RecordSet rs = client.query(null, stmt);
		
		try {
			int count = 0;
			
			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				int result = record.getInt(binName);
				
				console.info("Record found: ns=%s set=%s bin=%s digest=%s value=%s",
					key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);
				
				count++;
			}
			
			if (count != 5) {
				console.error("Query count mismatch. Expected 5. Received " + count);			
			}
		}
		finally {
			rs.close();
		}
	}
}
