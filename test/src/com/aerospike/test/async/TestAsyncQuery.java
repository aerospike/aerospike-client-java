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
package com.aerospike.test.async;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

public class TestAsyncQuery extends TestAsync {
	private static final String indexName = "asqindex";
	private static final String keyPrefix = "asqkey";
	private static final String binName = args.getBinName("asqbin");
	private static final int size = 50;

	@BeforeClass
	public static void initialize() {
		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client.createIndex(policy, args.namespace, args.set, indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	@AfterClass
	public static void destroy() {
		client.dropIndex(null, args.namespace, args.set, indexName);
	}

	@Test
	public void asyncQuery() {
		final AtomicInteger count = new AtomicInteger();

		for (int i = 1; i <= size; i++) {
			final Key key = new Key(args.namespace, args.set, keyPrefix + i);
			Bin bin = new Bin(binName, i);

			client.put(eventLoop, new WriteListener() {
				public void onSuccess(final Key key) {
					if (count.incrementAndGet() == size) {
						runQuery();
					}
				}

				public void onFailure(AerospikeException e) {
					setError(e);
					notifyComplete();
				}
			}, null, key, bin);
		}

		waitTillComplete();
	}

	private void runQuery() {
		int begin = 26;
		int end = 34;

		Statement stmt = new Statement();
		stmt.setNamespace(args.namespace);
		stmt.setSetName(args.set);
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));

		final AtomicInteger count = new AtomicInteger();

		client.query(eventLoop, new RecordSequenceListener() {
			public void onRecord(Key key, Record record) throws AerospikeException {
				int result = record.getInt(binName);
				assertBetween(26, 34, result);
				count.incrementAndGet();
			}

			public void onSuccess() {
				int size = count.get();
				assertEquals(9, size);
				notifyComplete();
			}

			public void onFailure(AerospikeException e) {
				setError(e);
				notifyComplete();
			}

		}, null, stmt);
	}
}
