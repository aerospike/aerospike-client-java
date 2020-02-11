/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class QueryCollection extends Example {

	public QueryCollection(Console console) {
		super(console);
	}

	/**
	 * Query records using a map index.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasUdf) {
			console.info("Query functions are not supported by the connected Aerospike server.");
			return;
		}

		String indexName = "mapkey_index";
		String keyPrefix = "qkey";
		String mapKeyPrefix = "mkey";
		String mapValuePrefix = "qvalue";
		String binName = params.getBinName("map_bin");
		int size = 20;

		// create collection index on mapKey
		createIndex(client, params, indexName, binName);
		// insert records with maps, where the map has 3 mapKeys <mapKeyPrefix>1, <mapKeyPrefix>2, <mapKeyPrefix>3
		writeRecords(client, params, keyPrefix, binName, mapKeyPrefix, mapValuePrefix, size);
		// query on mapKey <mapKeyPrefix>2
		runQuery(client, params, indexName, binName, mapKeyPrefix+2);
		client.dropIndex(params.policy, params.namespace, params.set, indexName);
	}

	private void createIndex(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create mapkeys index: ns=%s set=%s index=%s bin=%s",
			params.namespace, params.set, indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.STRING, IndexCollectionType.MAPKEYS);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void writeRecords(
		AerospikeClient client,
		Parameters params,
		String keyPrefix,
		String binName,
		String mapKeyPrefix,
		String valuePrefix,
		int size
	) throws Exception {
		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			HashMap<String,String> map = new HashMap<String,String>();

			map.put(mapKeyPrefix+1, valuePrefix+i);
			if (i%2 == 0) {
				map.put(mapKeyPrefix+2, valuePrefix+i);
			}
			if (i%3 == 0 ) {
				map.put(mapKeyPrefix+3, valuePrefix+i);
			}

			Bin bin = new Bin(binName, map);
			client.put(params.writePolicy, key, bin);

			/*
			console.info("Put: ns=%s set=%s key=%s bin=%s value=%s",
				key.namespace, key.setName, key.userKey, bin.name, bin.value);
			*/
		}
	}

	private void runQuery(
		AerospikeClient client,
		Parameters params,
		String indexName,
		String binName,
		String queryMapKey
	) throws Exception {

		console.info("Query for: ns=%s set=%s index=%s bin=%s mapkey contains=%s",
			params.namespace, params.set, indexName, binName, queryMapKey);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.contains(binName, IndexCollectionType.MAPKEYS, queryMapKey));

		RecordSet rs = client.query(null, stmt);

		try {
			int count = 0;

			while (rs.next()) {
				//Key key = rs.getKey();
				Record record = rs.getRecord();
				Map<?,?> result = (Map<?,?>)record.getValue(binName);

				if (result.containsKey(queryMapKey)) {
					/*console.info("Record found: ns=%s set=%s bin=%s key=%s value=%s",
						key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);
					*/
				}
				else {
					console.error("Query mismatch: Expected mapKey %s. Received %s.", queryMapKey, result);
				}
				count++;
			}

			if (count == 0) {
				console.error("Query failed. No records returned.");
			} else {
				console.info("Number of records %d",count);
			}
		}
		finally {
			rs.close();
		}
	}
}
