package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

public class QueryExecute extends Example {

	public QueryExecute(Console console) {
		super(console);
	}

	/**
	 * Apply user defined function on records that match the query filter.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Query functions are not supported by the connected Aerospike server.");
			return;
		}
		String indexName = "qeindex1";
		String keyPrefix = "qekey";
		String binName = params.getBinName("qebin");  
		int size = 10;

		client.register(params.policy, "udf/record_example.lua", "record_example.lua", Language.LUA);
		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, binName, size);
		runQueryExecute(client, params, indexName, binName);
		validateRecords(client, params, indexName, binName, size);
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
		console.info("Write " + size + " records.");

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);			
			client.put(params.writePolicy, key, bin);
		}
	}

	private void runQueryExecute(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {		
		Value begin = Value.get(5);
		Value end = Value.get(9);
		
		console.info("Add 100 to: ns=%s set=%s index=%s bin=%s >= %s <= %s",
			params.namespace, params.set, indexName, binName, begin, end);			
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setIndexName(indexName);
		stmt.setBinNames(binName);
		stmt.setFilters(Filter.range(binName, begin, end));
		
		client.execute(params.policy, stmt, "record_example", "addBin", Value.get(binName), Value.get(100));
	}
	
	private void validateRecords(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName,
		int size
	) throws Exception {		
		Value begin = Value.get(1);
		Value end = Value.get(size + 100);
		
		console.info("Validate records");
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setIndexName(indexName);
		stmt.setBinNames(binName);
		stmt.setFilters(Filter.range(binName, begin, end));
		
		RecordSet rs = client.query(null, stmt);
		
		try {
			int count = 0;
			
			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				int result = (Integer)record.getValue(binName);
				
				console.info("Record found: ns=%s set=%s bin=%s digest=%s value=%s",
					key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);
				
				String s = Integer.toString(result);
				
				if (result >= 100) {
					int lastDigit = Integer.parseInt(s.substring(s.length()-1, s.length()));
					
					if (lastDigit < 5 || lastDigit > 9) {
						int expected = lastDigit;
						console.error("Data mismatch. Expected " + expected + ". Received " + result);
						break;
					}				
				}
				else {
					int lastDigit = Integer.parseInt(s.substring(s.length()-1, s.length()));
					
					if (lastDigit >= 5 && lastDigit <= 9) {
						int expected = lastDigit + 100;
						console.error("Data mismatch. Expected " + expected + ". Received " + result);
						break;
					}				
				}
				count++;
			}
			
			if (count != size) {
				console.error("Query count mismatch. Expected " + size + ". Received " + count);			
			}
		}
		finally {
			rs.close();
		}
	}
}
