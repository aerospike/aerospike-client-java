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
package com.aerospike.client.command;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.Batch.BatchCommand;
import com.aerospike.client.policy.BatchPolicy;

public final class BatchExecutor {

	public static void execute(Cluster cluster, BatchPolicy policy, BatchCommand[] commands, BatchStatus status) {
		if (policy.maxConcurrentThreads == 1 || commands.length <= 1) {
			// Run batch requests sequentially in same thread.
			for (BatchCommand command : commands) {
				try {
					command.execute();
				}
				catch (AerospikeException ae) {
					// Set error/inDoubt for keys associated this batch command when
					// the command was not retried and split. If a split retry occurred,
					// those new subcommands have already set error/inDoubt on the affected
					// subset of keys.
					if (! command.splitRetry) {
						command.setError(ae.getResultCode(), ae.getInDoubt());
					}
					status.setException(ae);

					if (!policy.respondAllKeys) {
						throw ae;
					}
				}
				catch (RuntimeException re) {
					if (! command.splitRetry) {
						command.setError(ResultCode.CLIENT_ERROR, true);
					}
					status.setException(re);

					if (!policy.respondAllKeys) {
						throw re;
					}
				}
			}
			status.checkException();
			return;
		}

		// Run batch requests in parallel in separate threads.
		BatchExecutor executor = new BatchExecutor(cluster, policy, commands, status);
		executor.execute();
	}

	private final BatchStatus status;
	private final ExecutorService threadPool;
	private final AtomicBoolean done;
	private final AtomicInteger completedCount;
	private final BatchCommand[] commands;
	private final int maxConcurrentThreads;
	private boolean completed;

	private BatchExecutor(Cluster cluster, BatchPolicy policy, BatchCommand[] commands, BatchStatus status) {
		this.commands = commands;
		this.status = status;
		this.threadPool = cluster.getThreadPool();
		this.done = new AtomicBoolean();
		this.completedCount = new AtomicInteger();
		this.maxConcurrentThreads = (policy.maxConcurrentThreads == 0 || policy.maxConcurrentThreads >= commands.length)?
									commands.length : policy.maxConcurrentThreads;
	}

	void execute() {
		// Start threads.
		for (int i = 0; i < maxConcurrentThreads; i++) {
			BatchCommand cmd = commands[i];
			cmd.parent = this;
			threadPool.execute(cmd);
		}

		// Multiple threads write to the batch record array/list, so one might think that memory barriers
		// are needed. That should not be necessary because of this synchronized waitTillComplete().
		waitTillComplete();

		// Throw an exception if an error occurred.
		status.checkException();
	}

	void onComplete() {
		int finished = completedCount.incrementAndGet();

		if (finished < commands.length) {
			int nextThread = finished + maxConcurrentThreads - 1;

			// Determine if a new thread needs to be started.
			if (nextThread < commands.length && ! done.get()) {
				// Start new thread.
				BatchCommand cmd = commands[nextThread];
				cmd.parent = this;
				threadPool.execute(cmd);
			}
		}
		else {
			// Ensure executor succeeds or fails exactly once.
			if (done.compareAndSet(false, true)) {
				notifyCompleted();
			}
		}
	}

	boolean isDone() {
		return done.get();
	}

	private synchronized void waitTillComplete() {
		while (! completed) {
			try {
				super.wait();
			}
			catch (InterruptedException ie) {
			}
		}
	}

	private synchronized void notifyCompleted() {
		completed = true;
		super.notify();
	}
}
