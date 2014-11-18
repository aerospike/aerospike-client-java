/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;

public final class Executor {
	private final List<ExecutorThread> threads;
	private final ExecutorService threadPool;
	private volatile Exception exception;
	private final AtomicBoolean done;
	private final AtomicInteger completedCount;
	private int maxConcurrentThreads;
	private boolean completed;

	public Executor(Cluster cluster, int capacity) {
		threads = new ArrayList<ExecutorThread>(capacity);
		threadPool = cluster.getThreadPool();
		done = new AtomicBoolean();
		completedCount = new AtomicInteger();
	}

	public void addCommand(MultiCommand command) {
		threads.add(new ExecutorThread(command));
	}

	public void execute(int maxConcurrent) throws AerospikeException {
		// Initialize maximum number of nodes to query in parallel.
		maxConcurrentThreads = (maxConcurrent == 0 || maxConcurrent >= threads.size())? threads.size() : maxConcurrent;

		// Start threads.
		for (int i = 0; i < maxConcurrentThreads; i++) {
			threadPool.execute(threads.get(i));
		}
		waitTillComplete();
		
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
			int nextThread = finished + maxConcurrentThreads - 1;

			// Determine if a new thread needs to be started.
			if (nextThread < threads.size() && ! done.get()) {
				// Start new thread.
				threadPool.execute(threads.get(nextThread));
			}
		}
		else {
			// Ensure executor succeeds or fails exactly once.
			if (done.compareAndSet(false, true)) {				
				notifyCompleted();
			}
		}
	}

	private void stopThreads(Exception cause) {
		// Ensure executor succeeds or fails exactly once.
		if (done.compareAndSet(false, true)) {
	    	exception = cause;  		
			
			// Send stop signal to threads.
			for (ExecutorThread thread : threads) {
				thread.stop();
			}					
			notifyCompleted();
		}
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
			catch (Exception e) {
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
