/*
 * Copyright 2012-2016 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.util.Util;

/**
 * Task used to poll for server task completion.
 */
public abstract class Task {
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
		long deadline = 0;
		RuntimeException exception = null;
		
		while (! done) {
			// Only check for timeout on successive iterations.
			if (deadline == 0) {
				deadline = System.currentTimeMillis() + policy.timeout;
			}
			else {
				if (policy.timeout != 0 && System.currentTimeMillis() + sleepInterval > deadline) {
					if (exception != null) {
						// Use last exception received from queryIfDone().
						throw exception;
					}
					else {
						throw new AerospikeException.Timeout();
					}
				}
			}
			Util.sleep(sleepInterval);
			
			try {
				done = queryIfDone();			
			}
			catch (DoneException de) {
				// Throw exception immediately.
				throw de;
			}
			catch (RuntimeException re) {
				// Some tasks may initially give errors and then eventually succeed.
				// Store exception and continue till timeout. 
				exception = re;
			}
		}
	}

	/**
	 * Has task completed.
	 */
	public final boolean isDone() {
		if (done) {
			return true;
		}
		done = queryIfDone();
		return done;
	}
	
	/**
	 * Query all nodes for task completion status.
	 */
	protected abstract boolean queryIfDone();
	
	public static class DoneException extends AerospikeException {
		private static final long serialVersionUID = 1L;
		
		public DoneException(String message) {
			super(message);
		}
	}	
}
