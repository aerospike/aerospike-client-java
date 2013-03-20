package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Operator;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

public class Query extends Example {

	public Query(Console console) {
		super(console);
	}

	/**
	 * Create secondary index and query on it.
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
		//dropIndex(client, params, indexName);
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

	private void createIndex(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s, bin=%s",
			params.namespace, params.set, indexName, binName);			
		
		Policy policy = new Policy();
		policy.timeout = 0; // Do not timeout on index create.
		client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.STRING);
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName,
		String valuePrefix
	) throws Exception {
		
		Value filter = Value.get(valuePrefix + 3);
		
		console.info("Query for: ns=%s set=%s index=%s bin=%s filter=%s",
			params.namespace, params.set, indexName, binName, filter);			
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setIndexName(indexName);
		stmt.setBinNames(binName);
		stmt.setFilters(Filter.create(binName, Operator.EQUALS, filter));
		
		RecordSet rs = client.query(null, stmt);
		
		try {
			int count = 0;
			
			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				String result = (String)record.getValue(binName);
				
				if (result.equals(filter.getObject())) {
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
	
	/*
	private void dropIndex(
		AerospikeClient client,
		Parameters params,
		String indexName
	) throws Exception {
		console.info("Drop index: ns=%s set=%s index=%s",
			params.namespace, params.set, indexName);			
		
		client.dropIndex(null, params.namespace, params.set, indexName);		
	}
	*/
}
