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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.util.Util;

public final class QueryPartitionExecutor implements IQueryExecutor, Runnable {

	private final Cluster cluster;
	private final QueryPolicy policy;
	private final Statement statement;
	private final PartitionTracker tracker;
	private final RecordSet recordSet;
	private final List<QueryThread> threads;
	private final AtomicInteger completedCount;
	private final AtomicBoolean done;
	private volatile Throwable exception;
	private int maxConcurrentThreads;

	public QueryPartitionExecutor(
		Cluster cluster,
		QueryPolicy policy,
		Statement statement,
		int nodeCapacity,
		PartitionTracker tracker
	) {
		this.cluster = cluster;
		this.policy = policy;
		this.statement = statement;
		this.tracker = tracker;
		this.recordSet = new RecordSet(this, policy.recordQueueSize);
		this.threads = new ArrayList<QueryThread>(nodeCapacity);
		this.completedCount = new AtomicInteger();
		this.done = new AtomicBoolean();

		cluster.addCommandCount();
		cluster.threadFactory.newThread(this).start();
	}

	public void run() {
		try {
			execute();
		}
		catch (Throwable e) {
			stopThreads(e);
		}
	}

	private void execute() {
		TaskGen task = new TaskGen(statement);
		long taskId = task.getId();
		int retryInterval = policy.sleepBetweenRetries;
		double sleepMultiplier = policy.sleepMultiplier;

		while (true) {
			List<NodePartitions> list = tracker.assignPartitionsToNodes(cluster, statement.namespace);

			// Initialize maximum number of nodes to query in parallel.
			maxConcurrentThreads = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= list.size()) ? list.size() : policy.maxConcurrentNodes;

			boolean parallel = maxConcurrentThreads > 1 && list.size() > 1;
			ExecutorService es = null;

			synchronized(threads) {
				// RecordSet thread may have aborted query, so check done under lock.
				if (done.get()) {
					break;
				}

				threads.clear();

				if (parallel) {
					es = Executors.newThreadPerTaskExecutor(cluster.threadFactory);

					for (NodePartitions nodePartitions : list) {
						MultiCommand command = new QueryPartitionCommand(cluster, policy, statement, taskId, recordSet, tracker, nodePartitions);
						threads.add(new QueryThread(command));
					}

					for (int i = 0; i < maxConcurrentThreads; i++) {
						es.execute(threads.get(i));
					}
				}
			}

			if (parallel) {
				// Wait till virtual threads complete.
				es.close();
			}
			else {
				for (NodePartitions nodePartitions : list) {
					MultiCommand command = new QueryPartitionCommand(cluster, policy, statement, taskId, recordSet, tracker, nodePartitions);
					command.execute();
				}
			}

			if (exception != null) {
				break;
			}

			// Set done to false so RecordSet thread has chance to close early again.
			done.set(false);

			if (tracker.isComplete(cluster, policy)) {
				// All partitions received.
				recordSet.put(RecordSet.END);
				break;
			}

			if (retryInterval > 0) {
				// Sleep before trying again.
				Util.sleep(retryInterval);
				if (sleepMultiplier > 1) {
					retryInterval = (int) Math.round(sleepMultiplier * retryInterval);
				}
			}

			completedCount.set(0);
			exception = null;

			// taskId must be reset on next pass to avoid server duplicate query detection.
			taskId = task.nextId();
		}
	}

	private final void threadCompleted() {
		int finished = completedCount.incrementAndGet();

		if (finished < threads.size()) {
			int next = finished + maxConcurrentThreads - 1;

			// Determine if a new command needs to be started.
			if (next < threads.size() && ! done.get()) {
				// Start new command in existing thread.
				threads.get(next).run();
			}
		}
	}

	@Override
	public final void stopThreads(Throwable cause) {
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
			tracker.partitionError();
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
}
