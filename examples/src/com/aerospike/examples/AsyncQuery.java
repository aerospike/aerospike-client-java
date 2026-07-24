/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.util.Util;

public class AsyncQuery extends AsyncExample {
	/**
	 * Asynchronous query example.
	 */
	@Override
	public void runExample() {
		String indexName = "asqindex";
		String keyPrefix = "asqkey";
		String binName = "asqbin";
		int size = 50;

		createIndex(indexName, binName);
		beginRun();
		runQueryExample(keyPrefix, binName, size);

		// Do not drop index because after native client tests run.
		//client().dropIndex(readPolicy(), namespace(), set(), indexName);
	}

	private void createIndex(String indexName, String binName) {
		console.info("Create index: ns=%s set=%s index=%s bin=%s",
			namespace(), set(), indexName, binName);

		Policy policy = new Policy();
		policy.socketTimeout = 0; // Do not timeout on index create.

		try {
			IndexTask task = client().createIndex(policy, namespace(), set(), indexName, binName, IndexType.NUMERIC);
			task.waitTillComplete();
		}
		catch (AerospikeException ae) {
			if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
				throw ae;
			}
		}
	}

	private void runQueryExample(final String keyPrefix, final String binName, final int size) {
		console.info("Write " + size + " records.");
		WritePhase listener = new WritePhase(binName, size);
		int launched = 0;

		try {
			for (int i = 1; i <= size; i++) {
				Key key = new Key(namespace(), set(), keyPrefix + i);
				Bin bin = new Bin(binName, i);

				launched++;
				client().put(eventLoop(), listener, writePolicy(), key, bin);
			}
		}
		catch (Throwable t) {
			listener.onLaunchFailure(launched, t);
		}
	}

	private void runQuery(final String binName) throws Exception {
		final int begin = 26;
		final int end = 34;

		console.info("Query for: ns=%s set=%s bin=%s >= %s <= %s",
			namespace(), set(), binName, begin, end);

		Statement stmt = new Statement();
		stmt.setNamespace(namespace());
		stmt.setSetName(set());
		stmt.setBinNames(binName);
		stmt.setFilter(Filter.range(binName, begin, end));

		final AtomicInteger count = new AtomicInteger();
		final Throwable[] validationFailure = new Throwable[1];

		client().query(eventLoop(), new RecordSequenceListener() {
			public void onRecord(Key key, Record record) throws AerospikeException {
				int result = record.getInt(binName);

				console.info("Record found: ns=%s set=%s bin=%s digest=%s value=%s",
					key.namespace, key.setName, binName, Buffer.bytesToHexString(key.digest), result);

				if (result < begin || result > end) {
					validationFailure[0] = new IllegalStateException(
						"Query result out of range: " + result + " for key " + key.userKey);
				}
				count.incrementAndGet();
			}

			public void onSuccess() {
				int returned = count.get();

				if (validationFailure[0] != null) {
					failRun(validationFailure[0]);
				}
				else if (returned != 9) {
					failRun(new IllegalStateException(
						"Query count mismatch. Expected 9. Received " + returned));
				}
				else {
					completeRun();
				}
			}

			public void onFailure(AerospikeException e) {
				failRun(e);
			}

		}, null, stmt);
	}

	private class WritePhase implements WriteListener {
		private final String binName;
		private int launched;
		private int completed;
		private Throwable failure;

		private WritePhase(String binName, int launched) {
			this.binName = binName;
			this.launched = launched;
		}

		public void onSuccess(Key key) {
			onWriteComplete(null);
		}

		public void onFailure(AerospikeException e) {
			onWriteComplete(e);
		}

		private void onLaunchFailure(int launched, Throwable t) {
			this.launched = launched;

			if (failure == null) {
				failure = t;
			}

			if (launched == 0) {
				failRun(failure);
			}
			else if (completed == launched) {
				finish();
			}
		}

		private void onWriteComplete(Throwable t) {
			if (t != null && failure == null) {
				failure = t;
			}

			if (++completed == launched) {
				finish();
			}
		}

		private void finish() {
			if (failure != null) {
				failRun(failure);
				return;
			}

			try {
				beginRun();

				try {
					runQuery(binName);
					completeRun();
				}
				catch (Throwable t) {
					failRun(t);
					failRun(t);
				}
			}
			catch (Throwable t) {
				failRun(t);
			}
		}
	}
}
