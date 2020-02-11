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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;

public abstract class AsyncMultiExecutor {

	private final EventLoop eventLoop;
	final Cluster cluster;
	private AsyncMultiCommand[] commands;
	private String namespace;
	private long clusterKey;
	private int maxConcurrent;
	private int completedCount;  // Not atomic because all commands run on same event loop thread.
    boolean done;

	public AsyncMultiExecutor(EventLoop eventLoop, Cluster cluster) {
		this.eventLoop = eventLoop;
		this.cluster = cluster;
	}

	public void execute(AsyncMultiCommand[] commands, int maxConcurrent) {
		this.commands = commands;
		this.maxConcurrent = (maxConcurrent == 0 || maxConcurrent >= commands.length) ? commands.length : maxConcurrent;

		for (int i = 0; i < this.maxConcurrent; i++) {
			eventLoop.execute(cluster, commands[i]);
		}
	}

	public void executeBatchRetry(AsyncMultiCommand[] cmds, AsyncMultiCommand orig, Runnable other, long deadline) {
		// Create new commands array.
		AsyncMultiCommand[] target = new AsyncMultiCommand[commands.length + cmds.length - 1];
		int count = 0;

		for (AsyncMultiCommand cmd : commands) {
			if (cmd != orig) {
				target[count++] = cmd;
			}
		}

		for (AsyncMultiCommand cmd : cmds) {
			target[count++] = cmd;
		}
		commands = target;

		// Batch executors always execute all commands at once.
		// Execute all new commands.
		maxConcurrent = commands.length;

		for (AsyncMultiCommand cmd : cmds) {
			eventLoop.executeBatchRetry(other, cmd, deadline);
		}
	}

	public void executeValidate(final AsyncMultiCommand[] commands, int maxConcurrent, final String namespace) {
		this.commands = commands;
		this.maxConcurrent = (maxConcurrent == 0 || maxConcurrent >= commands.length) ? commands.length : maxConcurrent;
		this.namespace = namespace;

		final int max = this.maxConcurrent;

		AsyncQueryValidate.validateBegin(cluster, eventLoop, new AsyncQueryValidate.BeginListener() {
			@Override
			public void onSuccess(long key) {
				clusterKey = key;
				eventLoop.execute(cluster, commands[0]);

				for (int i = 1; i < max; i++) {
					executeValidateCommand(commands[i]);
				}
			}

			@Override
			public void onFailure(AerospikeException ae) {
				initFailure(ae);
			}
		}, commands[0].node, namespace);
	}

	private final void executeValidateCommand(final AsyncMultiCommand command) {
		AsyncQueryValidate.validate(cluster, eventLoop, new AsyncQueryValidate.Listener() {
			@Override
			public void onSuccess() {
				eventLoop.execute(cluster, command);
			}

			@Override
			public void onFailure(AerospikeException ae) {
				childFailure(ae);
			}
		}, command.node, namespace, clusterKey);
	}

	protected final void childSuccess(Node node) {
		if (clusterKey == 0) {
			queryComplete();
		}
		else {
			AsyncQueryValidate.validate(cluster, eventLoop, new AsyncQueryValidate.Listener() {
				@Override
				public void onSuccess() {
					queryComplete();
				}

				@Override
				public void onFailure(AerospikeException ae) {
					childFailure(ae);
				}
			}, node, namespace, clusterKey);
		}
	}

	private final void queryComplete() {
		completedCount++;

		if (completedCount < commands.length) {
			int nextThread = completedCount + maxConcurrent - 1;

			// Determine if a new command needs to be started.
			if (nextThread < commands.length && ! done) {
				// Start new command.
				if (clusterKey == 0) {
					eventLoop.execute(cluster, commands[nextThread]);
				}
				else {
					executeValidateCommand(commands[nextThread]);
				}
			}
		}
		else {
			// All commands complete. Notify success if an exception has not already occurred.
			if (! done) {
				done = true;
				onSuccess();
			}
		}
	}

	protected final void childFailure(AerospikeException ae) {
		// There is no need to stop commands if all commands have already completed.
		if (! done) {
			done = true;

			// Send stop signal to all commands.
			for (AsyncMultiCommand command : commands) {
				command.stop();
			}
			onFailure(ae);
		}
	}

	final void reset() {
		this.completedCount = 0;
		this.done = false;
	}

	private final void initFailure(AerospikeException ae) {
		onFailure(ae);
	}

	protected abstract void onSuccess();
	protected abstract void onFailure(AerospikeException ae);
}
