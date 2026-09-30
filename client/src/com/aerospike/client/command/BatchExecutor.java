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
package com.aerospike.client.command;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.BatchPolicy;

public final class BatchExecutor {

	public static void execute(Cluster cluster, BatchPolicy policy, IBatchCommand[] commands, BatchStatus status) {
		cluster.addCommandCount();

		if (commands.length <= 1) {
			// Run batch request in same thread.
			for (IBatchCommand command : commands) {
				try {
					command.execute();
				}
				catch (AerospikeException ae) {
					if (ae.getInDoubt()) {
						command.setInDoubt();
					}
					status.setException(ae);
				}
				catch (Throwable e) {
					command.setInDoubt();
					status.setException(new AerospikeException(e));
				}
			}
			status.checkException();
			return;
		}

		// Start virtual threads.
		ExecutorService es = Executors.newThreadPerTaskExecutor(cluster.threadFactory);

		try {
			for (IBatchCommand command : commands) {
				es.execute(command);
			}
		}
		finally {
			awaitCompletion(es, policy, status);
		}

		// Throw an exception if an error occurred.
		status.checkException();
	}

	/**
	 * Wait for the batch sub-commands, bounded by the policy's totalTimeout.
	 * <p>
	 * The default {@link ExecutorService#close()} waits in awaitTermination(1, DAYS) inside a
	 * "while (!terminated)" loop, so it re-arms indefinitely and cannot be bounded. A sub-command
	 * can outlive totalTimeout, because a single attempt is bounded per socket read by
	 * socketTimeout rather than in total, so that wait would hold the calling thread for as long
	 * as the sub-command runs.
	 */
	private static void awaitCompletion(ExecutorService es, BatchPolicy policy, BatchStatus status) {
		if (! await(es, policy.totalTimeout)) {
			// Abandon the sub-commands that are still running and notify the caller. BatchStatus
			// keeps the first exception, so a real sub-command error still takes precedence.
			status.setException(new AerospikeException.Timeout(policy, true));
		}
	}

	/**
	 * Wait for sub-commands to finish, bounded by remainingMillis. A remainingMillis of zero or
	 * less means wait without a limit, which matches a totalTimeout of zero. Returns true when
	 * every sub-command finished, false when the wait was cut short and the stragglers were
	 * abandoned.
	 */
	static boolean await(ExecutorService es, long remainingMillis) {
		es.shutdown();

		if (remainingMillis <= 0) {
			es.close();
			return true;
		}

		try {
			if (es.awaitTermination(remainingMillis, TimeUnit.MILLISECONDS)) {
				return true;
			}
		}
		catch (InterruptedException ie) {
			es.shutdownNow();
			Thread.currentThread().interrupt();
			throw new AerospikeException(ie);
		}

		es.shutdownNow();
		return false;
	}
}
