/*
 * Copyright 2012-2021 Aerospike, Inc.
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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.Util;

public class AsyncQuery extends AsyncExample {
	private static final String BinName = "asqbin";

	// TODO: Create multiple client instances.
	AerospikeClient client;
	final AtomicInteger queryComplete = new AtomicInteger();

	/**
	 * Asynchronous query example.
	 */
	@Override
	public void runExample(AerospikeClient client, EventLoop eventLoop) {
		String indexName = "asqindex";
		String keyPrefix = "asqkey";
		int size = 500000;

		/*
		createIndex(client, indexName, binName);
		console.info("Write " + size + " records.");

		for (int i = 1; i <= size; i++) {
			if (i % 20000 == 0) {
				console.info("Records=" + i);
			}
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);
			client.put(params.writePolicy, key, bin);
		}
		*/

		this.client = client;

		console.info("EventLoops=" + eventLoops.getSize());
		console.info("Run queries");

		for (int i = 0; i < 100; i++) {
			runQuery();
		}
		waitTillComplete();

		console.info("Close");
		client.close();
		console.info("Close done!");
		//client.dropIndex(policy, params.namespace, params.set, indexName);
	}
/*
	private void createIndex(
		AerospikeClient client,
		String indexName,
		String binName
	) {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			params.namespace, params.set, indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, params.namespace, params.set, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}
*/
	/*
	private void runQueryExample(
		final AerospikeClient client,
		final EventLoop eventLoop,
		final String keyPrefix,
		final String binName,
		final int size
	) {
		console.info("Write " + size + " records.");

		WriteListener listener = new WriteListener() {
			private int count = 0;

			public void onSuccess(final Key key) {
				// Use non-atomic increment because all writes are performed
				// in the same event loop thread.
				if (++count == size) {
					runQuery(client, eventLoop, binName);
				}
			}

			public void onFailure(AerospikeException e) {
				console.error("Failed to put: " + e.getMessage());
				notifyComplete();
			}
		};

		for (int i = 1; i <= size; i++) {
			Key key = new Key(params.namespace, params.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);

			client.put(eventLoop, listener, writePolicy, key, bin);
		}
	}*/

	private void runQuery() {
		int begin = 10;
		int end = 499999;

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(BinName);
		stmt.setFilter(Filter.range(BinName, begin, end));

		final EventLoop loop = eventLoops.next();

		client.query(loop, new RecordSequenceListener() {
			public void onRecord(Key key, Record record) {
			}

			public void onSuccess() {
				int c = queryComplete.incrementAndGet();
				console.info("Query Complete: " + c + ',' + loop.getIndex());

				if (c == 10) {
					notifyComplete();
				}
			}

			public void onFailure(AerospikeException e) {
				console.error("Query failed: " + Util.getErrorMessage(e));
				notifyComplete();
			}

		}, null, stmt);
	}
}
