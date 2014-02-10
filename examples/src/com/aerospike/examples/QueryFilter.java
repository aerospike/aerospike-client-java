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
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class QueryFilter extends Example {
	
	public QueryFilter(Console console) {
		super(console);
	}

	/**
	 * Query on a secondary index with a filter and then apply an additional filter in the 
	 * user defined function.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Query functions are not supported by the connected Aerospike server.");
			return;
		}
		String indexName = "profileindex";
		String keyPrefix = "profilekey";
		String binName = params.getBinName("name");  

		register(client, params);
		createIndex(client, params, indexName, binName);
		writeRecords(client, params, keyPrefix, binName);
		runQuery(client, params, indexName, binName);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);		
	}
	
	private void register(AerospikeClient client, Parameters params) throws Exception {
		RegisterTask task = client.register(params.policy, "udf/filter_example.lua", "filter_example.lua", Language.LUA);
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
		IndexTask task = client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.STRING);
		task.waitTillComplete();
	}

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName
	) throws Exception {
		writeRecord(client, params, keyPrefix + 1, "Charlie", "cpass");
		writeRecord(client, params, keyPrefix + 2, "Bill", "hknfpkj");
		writeRecord(client, params, keyPrefix + 3, "Doug", "dj6554");
	}

	private void writeRecord(
		AerospikeClient client,
		Parameters params,
		String userKey,
		String name,
		String password
	) throws Exception {
		Key key = new Key(params.namespace, params.set, userKey);
		Bin bin1 = new Bin("name", name);
		Bin bin2 = new Bin("password", password);
		console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
			key.namespace, key.setName, key.userKey, bin1.name, bin1.value);
		
		client.put(params.writePolicy, key, bin1, bin2);
	}

	@SuppressWarnings("unchecked")
	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		
		String nameFilter = "Bill";
		String passFilter = "hknfpkj";
		
		console.info("Query for: ns=%s set=%s index=%s name=%s pass=%s",
			params.namespace, params.set, indexName, nameFilter, passFilter);			
		
		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setFilters(Filter.equal(binName, nameFilter));
		
		// passFilter will be applied in filter_example.lua.
		ResultSet rs = client.queryAggregate(null, stmt, "filter_example", "profile_filter", Value.get(passFilter));
		
		try {
			int count = 0;
			
			while (rs.next()) {
				Map<String,Object> map = (Map<String,Object>)rs.getObject();				
				validate(map, "name", nameFilter);
				validate(map, "password", passFilter);
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
	
	private void validate(Map<String,Object> map, String name, Object expected) {
		Object val = map.get(name);
		
		if (val != null && val.equals(expected)) {
			console.info("Data matched: value=%s", expected);	
		}
		else {
			console.error("Data mismatch: Expected %s. Received %s.", expected, val);
		}
	}
}
