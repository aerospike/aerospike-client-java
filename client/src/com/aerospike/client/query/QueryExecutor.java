/*
 * Copyright 2012-2018 Aerospike, Inc.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.QueryPolicy;

public abstract class QueryExecutor {
	
	private final Cluster cluster;
	protected final QueryPolicy policy;
	protected final Statement statement;
	private final Node[] nodes;
	protected final ExecutorService threadPool;
	private final QueryThread[] threads;
	private final AtomicInteger completedCount;
    private final AtomicBoolean done;
	protected volatile Exception exception;
	private final int maxConcurrentNodes;
	
	public QueryExecutor(Cluster cluster, QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		this.cluster = cluster;
		this.policy = policy;
		this.statement = statement;
		this.completedCount = new AtomicInteger();
		this.done = new AtomicBoolean();
		
		if (node == null) {
			nodes = cluster.getNodes();
			
			if (nodes.length == 0) {
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.");
			}
		}
		else {
			nodes = new Node[] {node};
		}

		this.threadPool = cluster.getThreadPool();
		this.threads = new QueryThread[nodes.length];

		// Initialize maximum number of nodes to query in parallel.
		this.maxConcurrentNodes = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.length) ? threads.length : policy.maxConcurrentNodes;
	}
	
	protected final void initializeThreads() {
		// Detect cluster migrations when performing scan.
		long clusterKey = policy.failOnClusterChange ? QueryValidate.validateBegin(nodes[0], statement.namespace) : 0;	
		boolean first = true;
		
		// Initialize threads.
		for (int i = 0; i < nodes.length; i++) {
			MultiCommand command = createCommand(clusterKey, first);
			threads[i] = new QueryThread(nodes[i], command);
			first = false;
		}
	}

	protected final void startThreads() {
		// Start threads.
		for (int i = 0; i < maxConcurrentNodes; i++) {
			threadPool.execute(threads[i]);
		}
	}
	
	private final void threadCompleted() {
		int finished = completedCount.incrementAndGet();

		if (finished < threads.length) {
			int nextThread = finished + maxConcurrentNodes - 1;

			// Determine if a new thread needs to be started.
			if (nextThread < threads.length && ! done.get()) {
				// Start new thread.
				threadPool.execute(threads[nextThread]);
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

	protected final void stopThreads(Exception cause) {
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

	protected final void checkForException() throws AerospikeException {
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
		private final Node node;
		private final MultiCommand command;

		public QueryThread(Node node, MultiCommand command) {
			this.node = node;
			this.command = command;
		}

		public void run() {
			try {
				if (command.isValid()) {
					command.execute(cluster, policy, node);
				}
				threadCompleted();
			}
			catch (Exception e) {
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
	
	protected abstract MultiCommand createCommand(long clusterKey, boolean first);
	protected abstract void sendCancel();
	protected abstract void sendCompleted();
}
