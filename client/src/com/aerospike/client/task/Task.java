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
package com.aerospike.client.task;

import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

/**
 * Base type for server tasks (index creation, UDF registration, background query/scan execution) that complete asynchronously.
 *
 * <p>Use {@link #waitTillComplete()} or {@link #waitTillComplete(int)} to block until the task finishes, or
 * {@link #isDone()} to poll. Status is one of {@link #NOT_FOUND}, {@link #IN_PROGRESS}, or {@link #COMPLETE}.
 *
 * <p>Index creation task: create a secondary index and block until the server reports completion.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * IndexTask task = client.createIndex(null, "test", "users", "idx_status", "status", IndexType.STRING);
 * task.waitTillComplete();
 * }</pre>
 *
 * <p>UDF registration task: register a UDF package and poll with isDone() or waitTillComplete.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * RegisterTask task = client.register(null, "udf/myudfs.lua", "myudfs.lua", Language.LUA);
 * if (!task.isDone()) {
 *   task.waitTillComplete(10);
 * }
 * }</pre>
 *
 * <p>Background query/scan execute task: run a background query or scan and wait for completion.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * ExecuteTask task = client.execute(writePolicy, stmt, Operation.put(new Bin("processed", true)));
 * task.waitTillComplete();
 * }</pre>
 *
 * @see com.aerospike.client.task.IndexTask
 * @see com.aerospike.client.task.RegisterTask
 * @see com.aerospike.client.task.ExecuteTask
 */
public abstract class Task {
	/** Task status: task not found (may mean not started or already removed by server). */
	public static final int NOT_FOUND = 0;

	/** Task status: task is still running. */
	public static final int IN_PROGRESS = 1;

	/** Task status: task completed successfully. */
	public static final int COMPLETE = 2;

	protected final Cluster cluster;
	protected InfoPolicy policy;
	private boolean done;

	/**
	 * Constructs a task that will poll the given cluster for completion using the given policy (timeout, etc.).
	 *
	 * @param cluster the cluster to query for task status; must not be {@code null}
	 * @param policy policy for info commands used to poll (timeout, etc.); must not be {@code null}
	 */
	public Task(Cluster cluster, Policy policy) {
		this.cluster = cluster;
		this.policy = new InfoPolicy(policy);
		this.done = false;
	}

	/**
	 * Constructs a task that is already considered complete (e.g. for testing or no-op).
	 */
	public Task() {
		this.cluster = null;
		this.policy = null;
		this.done = true;
	}

	/**
	 * Blocks until the task completes or the policy timeout is reached (sleep interval 1 second).
	 *
	 * <p>Uses the timeout from the policy passed to the task constructor. If policy timeout is 0, waits indefinitely.
	 *
	 * @throws AerospikeException.Timeout	when the task does not complete before the policy timeout.
	 */
	public final void waitTillComplete() {
		taskWait(1000);
	}

	/**
	 * Blocks until the task completes or the policy timeout is reached.
	 *
	 * @param sleepInterval milliseconds to sleep between status polls
	 * @throws AerospikeException.Timeout	when the task does not complete before the policy timeout.
	 */
	public final void waitTillComplete(int sleepInterval) {
		taskWait(sleepInterval);
	}

	/**
	 * Blocks until the task completes or the given timeout is reached.
	 *
	 * @param sleepInterval milliseconds to sleep between status polls
	 * @param timeout maximum time to wait in milliseconds; 0 to wait indefinitely
	 * @throws AerospikeException.Timeout	when the task does not complete before the timeout.
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
		int iteration = 1;

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
					throw new AerospikeException.Timeout("Client timeout in taskWait()", iteration, policy.timeout, true);
				}
			}

			iteration++;
		} while (true);
	}

	/**
	 * Returns whether the task has completed (or is considered complete, e.g. NOT_FOUND after polling).
	 *
	 * @return {@code true} if the task is done, {@code false} if still in progress
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
	 * Queries the cluster for this task's completion status.
	 *
	 * @return {@link #NOT_FOUND}, {@link #IN_PROGRESS}, or {@link #COMPLETE}
	 */
	public abstract int queryStatus();
}
