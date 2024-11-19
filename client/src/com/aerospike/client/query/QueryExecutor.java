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
package com.aerospike.client.query;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.QueryPolicy;

public abstract class QueryExecutor implements IQueryExecutor {

	protected final Cluster cluster;
	protected final QueryPolicy policy;
	protected final Statement statement;
	protected final long taskId;
	private final Node[] nodes;
	private final QueryThread[] threads;
	private final AtomicInteger completedCount;
	private final AtomicBoolean done;
	protected volatile Throwable exception;
	private final int maxConcurrentNodes;

	public QueryExecutor(Cluster cluster, QueryPolicy policy, Statement statement, Node[] nodes) {
		this.cluster = cluster;
		this.policy = policy;
		this.statement = statement;
		this.taskId = statement.prepareTaskId();
		this.nodes = nodes;
		this.completedCount = new AtomicInteger();
		this.done = new AtomicBoolean();
		this.threads = new QueryThread[nodes.length];

		// Initialize maximum number of nodes to query in parallel.
		this.maxConcurrentNodes = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.length) ? threads.length : policy.maxConcurrentNodes;
		cluster.addCommandCount();
	}

	protected final void initializeThreads() {
		// Detect cluster migrations when performing scan.
		long clusterKey = policy.failOnClusterChange ? QueryValidate.validateBegin(nodes[0], statement.namespace, policy.infoTimeout) : 0;
		boolean first = true;

		// Initialize threads.
		for (int i = 0; i < nodes.length; i++) {
			MultiCommand command = createCommand(nodes[i], clusterKey, first);
			threads[i] = new QueryThread(command);
			first = false;
		}
	}

	protected final void startThreads() {
		// Start virtual threads.
		for (int i = 0; i < maxConcurrentNodes; i++) {
			cluster.threadFactory.newThread(threads[i]).start();
		}
	}

	private final void threadCompleted() {
		int finished = completedCount.incrementAndGet();

		if (finished < threads.length) {
			int next = finished + maxConcurrentNodes - 1;

			// Determine if a new command needs to be started.
			if (next < threads.length && ! done.get()) {
				// Start new command in existing thread.
				threads[next].run();
			}
		}
		else {
			// All threads complete.  Tell RecordSet thread to return complete to user
			// if an exception has not already occurred.
			if (done.compareAndSet(false, true)) {
				sendCompleted();
			}
		}
	}

	@Override
	public final void stopThreads(Throwable cause) {
		// There is no need to stop threads if all threads have already completed.
		if (done.compareAndSet(false, true)) {
			exception = cause;

			// Send stop signal to threads.
			for (QueryThread thread : threads) {
				thread.stop();
			}
			sendCancel();
		}
	}

	@Override
	public final void checkForException() {
		// Throw an exception if an error occurred.
		if (exception != null) {
			if (exception instanceof AerospikeException) {
				throw (AerospikeException)exception;
			}
			else {
				throw new AerospikeException(exception);
			}
		}
	}

	private final class QueryThread implements Runnable {
		private final MultiCommand command;

		public QueryThread(MultiCommand command) {
			this.command = command;
		}

		public void run() {
			try {
				if (command.isValid()) {
					command.executeAndValidate(policy.infoTimeout);
				}
				threadCompleted();
			}
			catch (Throwable e) {
				// Terminate other query threads.
				stopThreads(e);
			}
		}

		/**
		 * Send stop signal to each thread.
		 */
		public void stop() {
			command.stop();
		}
	}

	protected abstract MultiCommand createCommand(Node node, long clusterKey, boolean first);
	protected abstract void sendCancel();
	protected abstract void sendCompleted();
}
