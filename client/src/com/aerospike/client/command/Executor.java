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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;

public final class Executor {
	private final List<ExecutorThread> threads;
	private final ThreadFactory threadFactory;
	private volatile Throwable exception;
	private final AtomicBoolean done;
	private final AtomicInteger completedCount;
	private int maxConcurrentThreads;

	public Executor(Cluster cluster, int capacity) {
		threads = new ArrayList<ExecutorThread>(capacity);
		threadFactory = cluster.threadFactory;
		done = new AtomicBoolean();
		completedCount = new AtomicInteger();
	}

	public void addCommand(MultiCommand command) {
		threads.add(new ExecutorThread(command));
	}

	public void execute(int maxConcurrent) {
		// Initialize maximum number of nodes to query in parallel.
		maxConcurrentThreads = (maxConcurrent == 0 || maxConcurrent >= threads.size())? threads.size() : maxConcurrent;

		// Start virtual threads.
		try (ExecutorService es = Executors.newThreadPerTaskExecutor(threadFactory);) {
			for (int i = 0; i < maxConcurrentThreads; i++) {
				es.execute(threads.get(i));
			}
		}

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

	private void threadCompleted() {
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

	private void stopThreads(Throwable cause) {
		// Ensure executor succeeds or fails exactly once.
		if (done.compareAndSet(false, true)) {
			exception = cause;

			// Send stop signal to threads.
			for (ExecutorThread thread : threads) {
				thread.stop();
			}
		}
	}

	private final class ExecutorThread implements Runnable {
		private final MultiCommand command;

		public ExecutorThread(MultiCommand command) {
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
				// Terminate other scan threads.
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
