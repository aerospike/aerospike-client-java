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
package com.aerospike.client.task;

import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

/**
 * Task used to poll for server task completion.
 */
public abstract class Task {
	public static final int NOT_FOUND = 0;
	public static final int IN_PROGRESS = 1;
	public static final int COMPLETE = 2;

	protected final Cluster cluster;
	protected InfoPolicy policy;
	private boolean done;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public Task(Cluster cluster, Policy policy) {
		this.cluster = cluster;
		this.policy = new InfoPolicy(policy);
		this.done = false;
	}

	/**
	 * Initialize task that has already completed.
	 */
	public Task() {
		this.cluster = null;
		this.policy = null;
		this.done = true;
	}

	/**
	 * Wait for asynchronous task to complete using default sleep interval (1 second).
	 * The timeout is passed from the original task policy. If task is not complete by timeout,
	 * an exception is thrown.  Do not timeout if timeout set to zero.
	 */
	public final void waitTillComplete() {
		taskWait(1000);
	}

	/**
	 * Wait for asynchronous task to complete using given sleep interval in milliseconds.
	 * The timeout is passed from the original task policy. If task is not complete by timeout,
	 * an exception is thrown.  Do not timeout if policy timeout set to zero.
	 */
	public final void waitTillComplete(int sleepInterval) {
		taskWait(sleepInterval);
	}

	/**
	 * Wait for asynchronous task to complete using given sleep interval and timeout in milliseconds.
	 * If task is not complete by timeout, an exception is thrown.  Do not timeout if timeout set to
	 * zero.
	 */
	public final void waitTillComplete(int sleepInterval, int timeout) {
		policy = new InfoPolicy();
		policy.timeout = timeout;
		taskWait(sleepInterval);
	}

	/**
	 * Wait for asynchronous task to complete using given sleep interval in milliseconds.
	 * The timeout is passed from the original task policy. If task is not complete by timeout,
	 * an exception is thrown.  Do not timeout if policy timeout set to zero.
	 */
	private final void taskWait(int sleepInterval) {
		if (done) {
			return;
		}

		long deadline = (policy.timeout > 0)? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(policy.timeout) : 0L;

		do {
			// Sleep first to give task a chance to complete and help avoid case
			// where task hasn't started yet.
			Util.sleep(sleepInterval);

			int status = queryStatus();

			// The server can remove task listings immediately after completion
			// (especially for background query execute), so "NOT_FOUND" can
			// really mean complete. If not found and timeout not defined,
			// consider task complete.
			if (status == COMPLETE || (status == NOT_FOUND && policy.timeout == 0)) {
				done = true;
				return;
			}

			// Check for timeout.
			if (policy.timeout > 0 && System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sleepInterval) > deadline) {
				// Timeout has been reached or will be reached after next sleep.
				// Do not throw timeout exception when status is "NOT_FOUND" because the server will drop
				// background query execute task listings immediately after completion (which makes client
				// polling worthless).  This should be fixed by having server take an extra argument to query
				// execute command that says if server should wait till command is complete before responding
				// to client.
				if (status == NOT_FOUND) {
					done = true;
					return;
				}
				else {
					throw new AerospikeException.Timeout(policy.timeout, true);
				}
			}
		} while (true);
	}

	/**
	 * Has task completed.
	 */
	public final boolean isDone() {
		if (done) {
			return true;
		}

		int status = queryStatus();

		if (status == NOT_FOUND) {
			// The task may have not started yet.  Re-request status after a delay.
			Util.sleep(1000);
			status = queryStatus();
		}

		// The server can remove task listings immediately after completion
		// (especially for background query execute), so we must assume a
		// "not found" status means the task is complete.
		done = status != IN_PROGRESS;
		return done;
	}

	/**
	 * Query all nodes for task completion status.
	 */
	public abstract int queryStatus();
}
