package com.aerospike.examples;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Util;

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
		String binName1 = params.getBinName("qebin1");  
		String binName2 = params.getBinName("qebin2");  
		int size = 10;

		register(client, params);
		createIndex(client, params, indexName, binName1);
		writeRecords(client, params, keyPrefix, binName1, binName2, size);
		runQueryExecute(client, params, indexName, binName1, binName2);
		validateRecords(client, params, indexName, binName1, binName2, size);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);		
	}
	
	private void register(AerospikeClient client, Parameters params) throws Exception {
		client.register(params.policy, "udf/record_example.lua", "record_example.lua", Language.LUA);
		
		// The server UDF distribution to other nodes is done asynchronously.  Therefore, the server
		// may return before the UDF is available on all nodes.  Hard code sleep for now.
		// TODO: Fix server so control is only returned when UDF registration is complete.
		Util.sleep(1000);
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
		
		// The server index command distribution to other nodes is done asynchronously.  
		// Therefore, the server may return before the index is available on all nodes.  
		// Hard code sleep for now.
		// TODO: Fix server so control is only returned when index is available on all nodes.
		Util.sleep(1000);
	}

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName1,
		String binName2,
		int size
	) throws Exception {
		console.info("Write " + size + " records.");

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			client.put(params.writePolicy, key, new Bin(binName1, i), new Bin(binName2, i));
		}
	}

	private void runQueryExecute(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName1,
		String binName2
	) throws Exception {		
		Value begin = Value.get(3);
		Value end = Value.get(9);
		
		console.info("For ns=%s set=%s index=%s bin=%s >= %s <= %s",
			params.namespace, params.set, indexName, binName1, begin, end);			
		console.info("Even integers: add 100 to existing " + binName1);
		console.info("Multiple of 5: delete " + binName2 + " bin");
		console.info("Multiple of 9: delete record");
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilters(Filter.range(binName1, begin, end));
		
		client.execute(params.policy, stmt, "record_example", "processRecord", Value.get(binName1), Value.get(binName2), Value.get(100));
	}
	
	private void validateRecords(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName1,
		String binName2,
		int size
	) throws Exception {		
		Value begin = Value.get(1);
		Value end = Value.get(size + 100);
		
		console.info("Validate records");
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilters(Filter.range(binName1, begin, end));
		
		RecordSet rs = client.query(null, stmt);
		
		try {
			int[] expectedList = new int[] {1,2,3,104,5,106,7,108,-1,10};
			int expectedSize = size - 1;
			int count = 0;
			
			while (rs.next()) {
				Key key = rs.getKey();
				Record record = rs.getRecord();
				Integer value1 = (Integer)record.getValue(binName1);
				Integer value2 = (Integer)record.getValue(binName2);
				
				console.info("Record found: ns=%s set=%s bin1=%s value1=%s bin2=%s value2=%s",
					key.namespace, key.setName, binName1, value1, binName2, value2);
				
				if (value1 == null) {
					console.error("Data mismatch. value1 is null");
					break;
				}
				int val1 = value1;
				
				if (val1 == 9) {			
					console.error("Data mismatch. value1 " + val1 + " should not exist");
					break;
				}
				
				if (val1 == 5) {
					if (value2 != null) {
						console.error("Data mismatch. value2 " + value2 + " should be null");
						break;					
					}
				}
				else if (value1 != expectedList[value2-1]) {
					console.error("Data mismatch. Expected " + expectedList[value2-1] + ". Received " + value1);								
					break;					
				}
				count++;
			}
			
			if (count != expectedSize) {
				console.error("Query count mismatch. Expected " + expectedSize + ". Received " + count);			
			}
		}
		finally {
			rs.close();
		}
	}
}
