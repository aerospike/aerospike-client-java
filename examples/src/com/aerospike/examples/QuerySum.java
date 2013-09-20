package com.aerospike.examples;

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

public class QuerySum extends Example {

	public QuerySum(Console console) {
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
		String indexName = "aggindex";
		String keyPrefix = "aggkey";
		String binName = params.getBinName("aggbin");  
		int size = 10;

		client.register(params.policy, "udf/sum_example.lua", "sum_example.lua", Language.LUA);
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
		client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.NUMERIC);
	}

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		int size
	) throws Exception {
		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);
			
			console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, bin.value);
			
			client.put(params.writePolicy, key, bin);
		}
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		
		Value begin = Value.get(4);
		Value end = Value.get(7);
		
		console.info("Query for: ns=%s set=%s index=%s bin=%s >= %s <= %s",
			params.namespace, params.set, indexName, binName, begin, end);			
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setIndexName(indexName);
		stmt.setBinNames(binName);
		stmt.setFilters(Filter.range(binName, begin, end));
		
		ResultSet rs = client.queryAggregate(null, stmt, "sum_example", "sum_single_bin", Value.get(binName));
		
		try {
			int expected = 22; // 4 + 5 + 6 + 7
			int count = 0;
			
			while (rs.next()) {
				Object object = rs.getObject();
				int sum;
				
				if (object instanceof Integer) {
					sum = (Integer)rs.getObject();
				}
				else {
					console.error("Return value not an integer: " + object);
					continue;
				}
				
				if (expected == (int)sum) {
					console.info("Sum matched: value=%d", expected);
				}
				else {
					console.error("Sum mismatch: Expected %d. Received %d.", expected, (int)sum);
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
