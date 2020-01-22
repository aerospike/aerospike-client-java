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
package com.aerospike.client.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Util;

public final class QueryPartitionExecutor implements IQueryExecutor, Runnable {

	private final Cluster cluster;
	private final QueryPolicy policy;
	private final Statement statement;
	private final PartitionTracker tracker;
	private final RecordSet recordSet;
	private final ExecutorService threadPool;
	private final List<QueryThread> threads;
	private final AtomicInteger completedCount;
    private final AtomicBoolean done;
	private volatile Exception exception;
	private int maxConcurrentThreads;
	private boolean threadsComplete;

	public QueryPartitionExecutor(Cluster cluster, QueryPolicy policy, Statement statement, int nodeCapacity, PartitionTracker tracker) {
		this.cluster = cluster;
		this.policy = policy;
		statement.returnData = true;
		this.statement = statement;
		this.tracker = tracker;
		this.recordSet = new RecordSet(this, policy.recordQueueSize);
		this.threadPool = cluster.getThreadPool();
		this.threads = new ArrayList<QueryThread>(nodeCapacity);
		this.completedCount = new AtomicInteger();
		this.done = new AtomicBoolean();
		threadPool.execute(this);
	}

	public void run() {
		try {
			execute();
		}
		catch (Exception e) {
			stopThreads(e);
		}
	}

	private void execute() {
		while (true) {
			statement.taskId = RandomShift.instance().nextLong();

			List<NodePartitions> list = tracker.assignPartitionsToNodes(cluster, statement.namespace);

			// Initialize maximum number of nodes to query in parallel.
			maxConcurrentThreads = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= list.size()) ? list.size() : policy.maxConcurrentNodes;

			boolean parallel = maxConcurrentThreads > 1 && list.size() > 1;

			synchronized(threads) {
				// RecordSet thread may have aborted query, so check done under lock.
				if (done.get()) {
					break;
				}

				threads.clear();

				if (parallel) {
					for (NodePartitions nodePartitions : list) {
						MultiCommand command = new QueryPartitionCommand(cluster, nodePartitions.node, policy, statement, recordSet, tracker, nodePartitions);
						threads.add(new QueryThread(command));
					}

					for (int i = 0; i < maxConcurrentThreads; i++) {
						threadPool.execute(threads.get(i));
					}
				}
			}

			if (parallel) {
				waitTillComplete();
			}
			else {
				for (NodePartitions nodePartitions : list) {
					MultiCommand command = new QueryPartitionCommand(cluster, nodePartitions.node, policy, statement, recordSet, tracker, nodePartitions);
					command.execute();
				}
			}

			if (exception != null) {
				break;
			}

			if (tracker.isComplete(policy)) {
				// All partitions received.
				recordSet.put(RecordSet.END);
				break;
			}

			// Set done to false so RecordSet thread has chance to close early again.
			done.set(false);

			if (policy.sleepBetweenRetries > 0) {
				// Sleep before trying again.
				Util.sleep(policy.sleepBetweenRetries);
			}

			completedCount.set(0);
			threadsComplete = false;
			exception = null;
		}
	}

	private synchronized void waitTillComplete() {
		while (! threadsComplete) {
			try {
				super.wait();
			}
			catch (InterruptedException ie) {
			}
		}
	}

	private synchronized void notifyCompleted() {
		threadsComplete = true;
		super.notify();
	}

	private final void threadCompleted() {
		int finished = completedCount.incrementAndGet();

		if (finished < threads.size()) {
			int nextThread = finished + maxConcurrentThreads - 1;

			// Determine if a new thread needs to be started.
			if (nextThread < threads.size() && ! done.get()) {
				// Start new thread.
				threadPool.execute(threads.get(nextThread));
			}
		}
		else {
			// All threads complete.  Tell RecordSet thread to return complete to user
			// if an exception has not already occurred.
			if (done.compareAndSet(false, true)) {
				notifyCompleted();
			}
		}
	}

	@Override
	public final void stopThreads(Exception cause) {
		// There is no need to stop threads if all threads have already completed.
		if (done.compareAndSet(false, true)) {
	    	exception = cause;

			// Send stop signal to threads.
	    	// Must synchronize here because this method can be called from the main
	    	// RecordSet thread (user calls close() before retrieving all records)
	    	// which may conflict with the parallel QueryPartitionExecutor thread.
			synchronized(threads) {
				for (QueryThread thread : threads) {
					thread.stop();
				}
			}
			recordSet.abort();
			notifyCompleted();
		}
    }

	@Override
	public final void checkForException() {
		// Throw an exception if an error occurred.
		if (exception != null) {
			AerospikeException ae;

			if (exception instanceof AerospikeException) {
				ae = (AerospikeException)exception;
			}
			else {
				ae = new AerospikeException(exception);
			}
			ae.setIteration(tracker.iteration);
			throw ae;
		}
	}

	public RecordSet getRecordSet() {
		return recordSet;
	}

	private final class QueryThread implements Runnable {
		private final MultiCommand command;

		public QueryThread(MultiCommand command) {
			this.command = command;
		}

		public void run() {
			try {
				if (command.isValid()) {
					command.execute();
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
}
