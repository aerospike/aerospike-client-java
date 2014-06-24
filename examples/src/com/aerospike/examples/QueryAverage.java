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

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class QueryAverage extends Example {

	public QueryAverage(Console console) {
		super(console);
	}

	/**
	 * Create secondary index and query on it and apply aggregation user defined function.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Query functions are not supported by the connected Aerospike server.");
			return;
		}
		String indexName = "avgindex";
		String keyPrefix = "avgkey";
		String binName = params.getBinName("l2");  
		int size = 10;

		register(client, params);
		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, size);
		runQuery(client, params, indexName, binName);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);		
	}
	
	private void register(AerospikeClient client, Parameters params) throws Exception {
		RegisterTask task = client.register(params.policy, "udf/average_example.lua", "average_example.lua", Language.LUA);
		task.waitTillComplete();
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
		int size
	) throws Exception {
		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin("l1", i);
			
			console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, bin.value);
			
			client.put(params.writePolicy, key, bin, new Bin("l2", 1));
		}
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		
		console.info("Query for: ns=%s set=%s index=%s bin=%s",
			params.namespace, params.set, indexName, binName);			
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilters(Filter.range(binName, 0, 1000));
		
		ResultSet rs = client.queryAggregate(null, stmt, "average_example", "average");
		
		try {
			if (rs.next()) {
				Object obj = rs.getObject();
				
				if (obj instanceof Map<?,?>) {
					Map<?,?> map = (Map<?,?>)obj;
					long sum = (Long)map.get("sum");
					long count = (Long)map.get("count");
					double avg = (double) sum / count;
					console.info("Sum=" + sum + " Count=" + count + " Average=" + avg);					
					
					double expected = 5.5;
					if (avg != expected) {
						console.error("Data mismatch: Expected %s. Received %s.", expected, avg);
					}
				}
				else {			
					console.error("Unexpected object returned: " + obj);
				}
			}
			else {
				console.error("Query failed. No records returned.");
			}
		}
		finally {
			rs.close();
		}
	}
}
