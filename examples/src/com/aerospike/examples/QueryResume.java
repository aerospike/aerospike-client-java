/*
 * Copyright 2012-2022 Aerospike, Inc.
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

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class QueryResume extends Example {

	public QueryResume(Console console) {
		super(console);
	}

	/**
	 * Terminate a query and then resume query later.
	 */
	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		String indexName = "qridx";
		String binName = "bin";
		String setName = "qr";

		if (!params.useProxyClient) {
			createIndex(client, params, setName, indexName, binName);
		}
		writeRecords(client, params, setName, binName, 200);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(setName);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, 1, 200));

		PartitionFilter filter = PartitionFilter.all();
		AtomicInteger count = new AtomicInteger();

		console.info("Start query");

		try {
			client.query(null, stmt, filter, new QueryListener() {

				public void onRecord(Key key, Record record) {
					// Terminate query after 50 records.
					if (count.incrementAndGet() >= 50) {
						// Terminate query. The query last record key will not be set
						// and the current record will be returned again if the query resumes
						// at a later time. It's designed this way to handle errors where
						// the last record returned could not be processed (like a disk full
						// error on a backup).
						throw new AerospikeException.QueryTerminated();
					}
				}
			});
		}
		catch (AerospikeException.QueryTerminated e) {
			console.info("Query terminated as expected");
		}

		console.info("Records returned: " + count.get());
		count.set(0);

		// PartitionFilter could be serialized at this point.
		// Resume query now.
		console.info("Start query resume");

		client.query(null, stmt, filter, new QueryListener() {
			public void onRecord(Key key, Record record) {
				count.incrementAndGet();
			}
		});

		console.info("Records returned: " + count.get());
	}

	private void createIndex(
		IAerospikeClient client,
		Parameters params,
		String setName,
		String indexName,
		String binName
	) throws Exception {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			params.namespace, setName, indexName, binName);

		Policy policy = new Policy();

		try {
			IndexTask task = client.createIndex(policy, params.namespace, setName, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void writeRecords(
		IAerospikeClient client,
		Parameters params,
		String setName,
		String binName,
		int size
	) throws Exception {
		console.info("Write " + size + " records.");

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, setName, i);
			Bin bin = new Bin(binName, i);
			client.put(params.writePolicy, key, bin);
		}
	}
}
