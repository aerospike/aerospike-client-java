package com.aerospike.examples;

import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;

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

		client.register(params.policy, "udf/average_example.lua", "average_example.lua", Language.LUA);
		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, size);
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
		client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.NUMERIC);
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
		stmt.setIndexName(indexName);
		stmt.setFilters(Filter.range(binName, Value.get(0), Value.get(1000)));
		
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
						console.error("Data mismatch: Expected %d. Received %d.", expected, avg);
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
