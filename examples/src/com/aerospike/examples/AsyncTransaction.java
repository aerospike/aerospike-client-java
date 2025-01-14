/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.AbortStatus;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.CommitStatus;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.AbortListener;
import com.aerospike.client.listener.CommitListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Asynchronous multi-record transaction example.
 */
public class AsyncTransaction extends AsyncExample {
	@Override
	public void runExample(IAerospikeClient client, EventLoop eventLoop) {
		Txn txn = new Txn();

		console.info("Begin txn: " + txn.getId());
		put(client, txn, eventLoop);
	}

	public void put(IAerospikeClient client, Txn txn, EventLoop eventLoop) {
		console.info("Run put");

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;

		Key key = new Key(params.namespace, params.set, 1);

		WriteListener wl = new WriteListener() {
			public void onSuccess(final Key key) {
				putAnother(client, txn, eventLoop);
			}

			public void onFailure(AerospikeException e) {
				console.error("Failed to write: namespace=%s set=%s key=%s exception=%s",
						key.namespace, key.setName, key.userKey, e.getMessage());
				abort(client, txn, eventLoop);
			}
		};

		client.put(eventLoop, wl, wp, key, new Bin("a", "val1"));
	}

	public void putAnother(IAerospikeClient client, Txn txn, EventLoop eventLoop) {
		console.info("Run another put");

		WritePolicy wp = client.copyWritePolicyDefault();
		wp.txn = txn;

		Key key = new Key(params.namespace, params.set, 2);

		WriteListener wl = new WriteListener() {
			public void onSuccess(final Key key) {
				get(client, txn, eventLoop);
			}

			public void onFailure(AerospikeException e) {
				console.error("Failed to write: namespace=%s set=%s key=%s exception=%s",
					key.namespace, key.setName, key.userKey, e.getMessage());
				abort(client, txn, eventLoop);
			}
		};

		client.put(eventLoop, wl, wp, key, new Bin("b", "val2"));
	}

	public void get(IAerospikeClient client, Txn txn, EventLoop eventLoop) {
		console.info("Run get");

		Policy p = client.copyReadPolicyDefault();
		p.txn = txn;

		Key key = new Key(params.namespace, params.set, 3);

		RecordListener rl = new RecordListener() {
			public void onSuccess(Key key, Record record) {
				delete(client, txn, eventLoop);
			}

			public void onFailure(AerospikeException e) {
				console.error("Failed to read: namespace=%s set=%s key=%s exception=%s",
					key.namespace, key.setName, key.userKey, e.getMessage());
				abort(client, txn, eventLoop);
			}
		};

		client.get(eventLoop, rl, p, key);
	}

	public void delete(IAerospikeClient client, Txn txn, EventLoop eventLoop) {
		console.info("Run delete");

		WritePolicy dp = client.copyWritePolicyDefault();
		dp.txn = txn;
		dp.durableDelete = true;  // Required when running delete in a transaction.

		Key key = new Key(params.namespace, params.set, 3);

		DeleteListener dl = new DeleteListener() {
			public void onSuccess(final Key key, boolean existed) {
				commit(client, txn, eventLoop);
			}

			public void onFailure(AerospikeException e) {
				console.error("Failed to delete: namespace=%s set=%s key=%s exception=%s",
						key.namespace, key.setName, key.userKey, e.getMessage());
				abort(client, txn, eventLoop);
			}
		};

		client.delete(eventLoop, dl, dp, key);
	}

	public void commit(IAerospikeClient client, Txn txn, EventLoop eventLoop) {
		console.info("Run commit");

		CommitListener tcl = new CommitListener() {
			public void onSuccess(CommitStatus status) {
				console.info("Txn committed: " + txn.getId());
			}

			public void onFailure(AerospikeException.Commit ae) {
				console.error("Txn commit failed: " + txn.getId());
			}
		};

		client.commit(eventLoop, tcl, txn);
	}

	public void abort(IAerospikeClient client, Txn txn, EventLoop eventLoop) {
		console.info("Run abort");

		AbortListener tal = new AbortListener() {
			public void onSuccess(AbortStatus status) {
				console.error("Txn aborted: " + txn.getId());
			}
		};

		client.abort(eventLoop, tal, txn);
	}
}
