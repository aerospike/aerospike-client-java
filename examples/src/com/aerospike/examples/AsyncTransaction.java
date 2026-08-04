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
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
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
	private Throwable terminalFailure;

	@Override
	public void runExample() {
		final Txn txn = new Txn();
		terminalFailure = null;

		console.info("Begin txn: " + txn.getId());
		beginRun();

		try {
			new Sequence(txn, new Runner[] {
				new PutStep("Run put", txn, new Key(namespace(), set(), 1), new Bin("a", "val1")),
				new PutStep("Run another put", txn, new Key(namespace(), set(), 2), new Bin("b", "val2")),
				new GetStep(txn, new Key(namespace(), set(), 3)),
				new DeleteStep(txn, new Key(namespace(), set(), 3))
			}).runNext();
		}
		catch (Throwable t) {
			failRun(t);
		}
	}

	private void commit(final Txn txn) {
		console.info("Run commit");

		CommitListener tcl = new CommitListener() {
			public void onSuccess(CommitStatus status) {
				console.info("Txn committed: " + txn.getId());
				completeRun();
			}

			public void onFailure(AerospikeException.Commit ae) {
				failRun(ae);
			}
		};

		client().commit(eventLoop(), tcl, txn);
	}

	private void abort(final Txn txn, Throwable cause) {
		if (terminalFailure == null) {
			terminalFailure = cause;
		}

		console.info("Run abort");

		AbortListener tal = (AbortStatus status) -> {
			console.info("Txn aborted: " + txn.getId());
			failRun(terminalFailure);
		};

		try {
			client().abort(eventLoop(), tal, txn);
		}
		catch (Throwable t) {
			if (terminalFailure != null && terminalFailure != t) {
				terminalFailure.addSuppressed(t);
				failRun(terminalFailure);
			}
			else {
				failRun(t);
			}
		}
	}

	private class Sequence implements Listener {
		private final Txn txn;
		private final Runner[] runners;
		private int index = -1;

		private Sequence(Txn txn, Runner[] runners) {
			this.txn = txn;
			this.runners = runners;
		}

		private void runNext() {
			if (++index == runners.length) {
				try {
					commit(txn);
				}
				catch (Throwable t) {
					failRun(t);
				}
				return;
			}

			try {
				runners[index].run(this);
			}
			catch (Throwable t) {
				abort(txn, t);
			}
		}

		public void onSuccess() {
			runNext();
		}

		public void onFailure(Throwable failure) {
			abort(txn, failure);
		}
	}

	private class PutStep implements Runner {
		private final String label;
		private final Txn txn;
		private final Key key;
		private final Bin bin;

		private PutStep(String label, Txn txn, Key key, Bin bin) {
			this.label = label;
			this.txn = txn;
			this.key = key;
			this.bin = bin;
		}

		public void run(final Listener listener) {
			console.info(label);

			WritePolicy wp = client().copyWritePolicyDefault();
			wp.txn = txn;

			client().put(eventLoop(), new WriteListener() {
				public void onSuccess(Key key) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			}, wp, key, bin);
		}
	}

	private class GetStep implements Runner {
		private final Txn txn;
		private final Key key;

		private GetStep(Txn txn, Key key) {
			this.txn = txn;
			this.key = key;
		}

		public void run(final Listener listener) {
			console.info("Run get");

			Policy p = client().copyReadPolicyDefault();
			p.txn = txn;

			client().get(eventLoop(), new RecordListener() {
				public void onSuccess(Key key, Record record) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			}, p, key);
		}
	}

	private class DeleteStep implements Runner {
		private final Txn txn;
		private final Key key;

		private DeleteStep(Txn txn, Key key) {
			this.txn = txn;
			this.key = key;
		}

		public void run(final Listener listener) {
			console.info("Run delete");

			WritePolicy dp = client().copyWritePolicyDefault();
			dp.txn = txn;
			dp.durableDelete = true;

			client().delete(eventLoop(), new DeleteListener() {
				public void onSuccess(Key key, boolean existed) {
					listener.onSuccess();
				}

				public void onFailure(AerospikeException e) {
					listener.onFailure(e);
				}
			}, dp, key);
		}
	}

	private interface Runner {
		void run(Listener listener);
	}

	private interface Listener {
		void onSuccess();
		void onFailure(Throwable failure);
	}
}
