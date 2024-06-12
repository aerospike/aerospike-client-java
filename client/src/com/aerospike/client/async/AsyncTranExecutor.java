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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Tran;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.OperateArgs;
import com.aerospike.client.command.TranMonitor;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Util;
import java.util.ArrayList;
import java.util.List;

public abstract class AsyncTranExecutor {
	public static void execute(EventLoop eventLoop, Cluster cluster, WritePolicy policy, AsyncWriteBase command) {
		if (policy.tran == null) {
			// Command is not run under a MRT monitor. Run original command.
			eventLoop.execute(cluster, command);
			return;
		}

		Tran tran = policy.tran;
		Key cmdKey = command.key;

		if (tran.getWrites().contains(cmdKey)) {
			// MRT monitor already contains this key. Run original command.
			eventLoop.execute(cluster, command);
			return;
		}

		tran.setNamespace(cmdKey.namespace);

		// Add key to MRT monitor and then run original command.
		Operation[] ops = TranMonitor.getTranOps(tran, cmdKey);
		AsyncTranExecutor.Single ate = new AsyncTranExecutor.Single(eventLoop, cluster, command);
		ate.execute(policy, ops);
	}

	public static void executeBatch(
		BatchPolicy policy,
		AsyncBatchExecutor executor,
		AsyncCommand[] commands,
		Key[] keys
	) {
		if (policy.tran == null) {
			// Command is not run under a MRT monitor. Run original command.
			executor.execute(commands);
			return;
		}

		// Add write keys to MRT monitor and then run original command.
		Tran tran = policy.tran;
		ArrayList<Value> list = new ArrayList<>(keys.length);

		for (Key key : keys) {
			tran.setNamespace(key.namespace);
			list.add(Value.get(key.digest));
		}

		Operation[] ops = TranMonitor.getTranOps(tran, list);
		AsyncTranExecutor.Batch ate = new AsyncTranExecutor.Batch(executor, commands);
		ate.execute(policy, ops);
	}

	public static void executeBatch(
		BatchPolicy policy,
		AsyncBatchExecutor executor,
		AsyncCommand[] commands,
		List<BatchRecord> records
	) {
		if (policy.tran == null) {
			// Command is not run under a MRT monitor. Run original command.
			executor.execute(commands);
			return;
		}

		// Add write keys to MRT monitor and then run original command.
		Tran tran = policy.tran;
		ArrayList<Value> list = new ArrayList<>(records.size());

		for (BatchRecord br : records) {
			if (br.hasWrite) {
				Key key = br.key;
				tran.setNamespace(key.namespace);
				list.add(Value.get(key.digest));
			}
		}

		Operation[] ops = TranMonitor.getTranOps(tran, list);
		AsyncTranExecutor.Batch ate = new AsyncTranExecutor.Batch(executor, commands);
		ate.execute(policy, ops);
	}

	private static class Single extends AsyncTranExecutor {
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

	private static class Batch extends AsyncTranExecutor {
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

	private AsyncTranExecutor(EventLoop eventLoop, Cluster cluster) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
	}

	void execute(Policy policy, Operation[] ops) {
		Key tranKey = TranMonitor.getTranMonitorKey(policy.tran);
		WritePolicy wp = TranMonitor.copyTimeoutPolicy(policy);

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
				notifyFailure(new AerospikeException(ResultCode.TRAN_FAILED, "Failed to add key(s) to MRT monitor", ae));
			}
		};

		// Add write key(s) to MRT monitor.
		OperateArgs args = new OperateArgs(wp, null, null, ops);
		AsyncOperateWrite tranCommand = new AsyncOperateWrite(cluster, tranListener, tranKey, args);
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
