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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Txn;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.TxnMonitor;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import java.util.List;

public abstract class AsyncTxnMonitor {
	public static void execute(EventLoop eventLoop, Cluster cluster, WritePolicy policy, AsyncWriteBase command) {
		if (policy.txn == null) {
			// Command is not run under a transaction. Run original command.
			eventLoop.execute(cluster, command);
			return;
		}

		Txn txn = policy.txn;
		txn.verifyCommand();

		Key cmdKey = command.key;

		if (txn.getWrites().contains(cmdKey)) {
			// Transaction already contains this key. Run original command.
			eventLoop.execute(cluster, command);
			return;
		}

		// Add key to transaction monitor and then run original command.
		Operation[] ops = TxnMonitor.getTranOps(txn, cmdKey);
		AsyncTxnMonitor.Single ate = new AsyncTxnMonitor.Single(eventLoop, cluster, command);
		ate.execute(policy, ops);
	}

	public static void executeBatch(
		BatchPolicy policy,
		AsyncBatchExecutor executor,
		AsyncCommand[] commands,
		Key[] keys
	) {
		if (policy.txn == null) {
			// Command is not run under a transaction. Run original command.
			executor.execute(commands);
			return;
		}

		// Add write keys to transaction monitor and then run original command.
		Operation[] ops = TxnMonitor.getTranOps(policy.txn, keys);
		AsyncTxnMonitor.Batch ate = new AsyncTxnMonitor.Batch(executor, commands);
		ate.execute(policy, ops);
	}

	public static void executeBatch(
		BatchPolicy policy,
		AsyncBatchExecutor executor,
		AsyncCommand[] commands,
		List<BatchRecord> records
	) {
		if (policy.txn == null) {
			// Command is not run under a transaction. Run original command.
			executor.execute(commands);
			return;
		}

		// Add write keys to transaction monitor and then run original command.
		Operation[] ops = TxnMonitor.getTranOps(policy.txn, records);

		if (ops == null) {
			// Readonly batch does not need to add key digests. Run original command.
			executor.execute(commands);
			return;
		}

		AsyncTxnMonitor.Batch ate = new AsyncTxnMonitor.Batch(executor, commands);
		ate.execute(policy, ops);
	}

	private static class Single extends AsyncTxnMonitor {
		private final AsyncWriteBase command;

		private Single(EventLoop eventLoop, Cluster cluster, AsyncWriteBase command) {
			super(eventLoop, cluster);
			this.command = command;
		}

		@Override
		void runCommand() {
			eventLoop.execute(cluster, command);
		}

		@Override
		void onFailure(AerospikeException ae) {
			command.onFailure(ae);
		}
	}

	private static class Batch extends AsyncTxnMonitor {
		private final AsyncBatchExecutor executor;
		private final AsyncCommand[] commands;

		private Batch(AsyncBatchExecutor executor, AsyncCommand[] commands) {
			super(executor.eventLoop, executor.cluster);
			this.executor = executor;
			this.commands = commands;
		}

		@Override
		void runCommand() {
			executor.execute(commands);
		}

		@Override
		void onFailure(AerospikeException ae) {
			executor.onFailure(ae);
		}
	}

	final EventLoop eventLoop;
	final Cluster cluster;

	private AsyncTxnMonitor(EventLoop eventLoop, Cluster cluster) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
	}

	void execute(Policy policy, Operation[] ops) {
		Txn txn = policy.txn;
		Key tranKey = TxnMonitor.getTxnMonitorKey(txn);
		WritePolicy wp = TxnMonitor.copyTimeoutPolicy(policy);

		RecordListener tranListener = new RecordListener() {
			@Override
			public void onSuccess(Key key, Record record) {
				try {
					// Run original command.
					runCommand();
				}
				catch (AerospikeException ae) {
					notifyFailure(ae);
				}
				catch (Throwable t) {
					notifyFailure(new AerospikeException(t));
				}
			}

			@Override
			public void onFailure(AerospikeException ae) {
				notifyFailure(new AerospikeException(ResultCode.TXN_FAILED, "Failed to add key(s) to transaction monitor", ae));
			}
		};

		// Add write key(s) to transaction monitor.
		OperateArgs args = new OperateArgs(wp, null, null, ops);
		AsyncTxnAddKeys tranCommand = new AsyncTxnAddKeys(cluster, tranListener, tranKey, args, txn);
		eventLoop.execute(cluster, tranCommand);
	}

	private void notifyFailure(AerospikeException ae) {
		try {
			onFailure(ae);
		}
		catch (Throwable t) {
			Log.error("notifyCommandFailure onFailure() failed: " + Util.getStackTrace(t));
		}
	}

	abstract void onFailure(AerospikeException ae);
	abstract void runCommand();
}
