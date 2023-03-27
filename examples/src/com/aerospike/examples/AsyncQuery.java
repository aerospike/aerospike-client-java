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

import java.util.concurrent.CountDownLatch;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;

public class AsyncQuery extends AsyncExample {
	/**
	 * Asynchronous query example.
	 */
	@Override
	public void runExample(AerospikeClient client, EventLoop eventLoop) {
		final Statement queryStatement = buildQueueQueryStatement("pitr-ns1", "testset76", "as-backup-queue", 200);

        final CountDownLatch latch = new CountDownLatch(1);
        RecordSequenceListener listener = new RecordSequenceListener() {

           @Override
            public void onRecord(Key key, Record record) throws AerospikeException {
                System.out.println(key + ": " + record.bins);
            }

            @Override
            public void onSuccess() {
                System.out.println("Done");
                latch.countDown();
            }

            @Override
            public void onFailure(AerospikeException e) {
                e.printStackTrace();
                latch.countDown();
            }
		};

        client.query(null, listener, queryRecordsStatusNew(), queryStatement);

        try {
        	latch.await();
        }
        catch (InterruptedException ie) {
        	System.out.println("INTERRUPTED");
        }
	}

	private Statement buildQueueQueryStatement(String namespace, String set, String queueSet, long batchSize) {
        Statement queryStatement = new Statement();
        queryStatement.setMaxRecords(2L * batchSize);
        queryStatement.setNamespace(namespace);
        queryStatement.setSetName(queueSet);

        /* Get records from a specific source set */
        queryStatement.setFilter(Filter.equal("set", "testset76"));

        // ---> The following secondary index filter returns records
//        queryStatement.setFilter(Filter.equal("status", QueueRecord.QueueRecordStatus.NEW.getStatus()));
        return queryStatement;
    }

    private QueryPolicy queryRecordsStatusNew() {
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.shortQuery = false;
        queryPolicy.includeBinData = true;
        queryPolicy.filterExp = Exp.build(
                Exp.and(
                        Exp.eq(
                                Exp.stringBin("status"),
                                Exp.val("New")
                        ),
                        Exp.eq(
                                Exp.stringBin("set"),
                                Exp.val("testset76")
                        )
                ));
        return queryPolicy;
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
	}

	private void runQuery(AerospikeClient client, EventLoop eventLoop, final String binName) {
		int begin = 26;
		int end = 34;

		console.info("Query for: ns=%s set=%s bin=%s >= %s <= %s",
			params.namespace, params.set, binName, begin, end);

		Statement stmt = new Statement();
		stmt.setNamespace(params.namespace);
		stmt.setSetName(params.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));

		final AtomicInteger count = new AtomicInteger();

		client.query(eventLoop, new RecordSequenceListener() {
			public void onRecord(Key key, Record record) throws AerospikeException {
				int result = record.getInt(binName);

				console.info("Record found: ns=%s set=%s bin=%s digest=%s value=%s",
					key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);

				count.incrementAndGet();
			}

			public void onSuccess() {
				int size = count.get();

				if (size != 9) {
					console.error("Query count mismatch. Expected 9. Received " + size);
				}
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				console.error("Query failed: " + Util.getErrorMessage(e));
				notifyComplete();
			}

		}, null, stmt);
	}*/
}
