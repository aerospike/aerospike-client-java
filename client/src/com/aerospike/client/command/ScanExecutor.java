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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ScanPolicy;

public final class ScanExecutor {
	
	private final ExecutorService threadPool;
	private final ScanThread[] threads;
	private final AtomicInteger completedCount;
	private volatile Exception exception;
	private final int maxConcurrentNodes;
	private boolean completed;
	
	public ScanExecutor(Cluster cluster, Node[] nodes, ScanPolicy policy, String namespace, String setName, ScanCallback callback, String[] binNames) {
		this.completedCount = new AtomicInteger();
		this.threadPool = cluster.getThreadPool();
		
		// Initialize threads.		
		threads = new ScanThread[nodes.length];
		
		for (int i = 0; i < nodes.length; i++) {
			ScanCommand command = new ScanCommand(nodes[i], policy, namespace, setName, callback, binNames);
			threads[i] = new ScanThread(command);
		}
		
		// Initialize maximum number of nodes to query in parallel.
		maxConcurrentNodes = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.length)? threads.length : policy.maxConcurrentNodes;
	}
	
	public void scanParallel() throws AerospikeException {
		// Start threads.
		for (int i = 0; i < maxConcurrentNodes; i++) {
			threadPool.execute(threads[i]);
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

		if (finished < threads.length) {
			int nextThread = finished + maxConcurrentNodes - 1;

			// Determine if a new thread needs to be started.
			if (nextThread < threads.length) {
				// Start new thread.
				threadPool.execute(threads[nextThread]);
			}
		}
		else {
			notifyCompleted();
		}
	}

	private void stopThreads(Exception cause) {
    	synchronized (threads) {
    	   	if (exception != null) {
    	   		return;
    	   	}
	    	exception = cause;  		
    	}
    	
		for (ScanThread thread : threads) {
			try {
				thread.stop();
			}
			catch (Exception e) {
			}
		}
		notifyCompleted();
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

    private final class ScanThread implements Runnable {
		private final ScanCommand command;
		private Thread thread;

		public ScanThread(ScanCommand command) {
			this.command = command;
		}
		
		public void run() {
			thread = Thread.currentThread();
			
			try {
				if (command.isValid()) {
					command.execute();
				}
			}
			catch (Exception e) {
				// Terminate other scan threads.
				stopThreads(e);
			}
			
		   	if (exception == null) {
				threadCompleted();
		   	}
		}
		
		public void stop() {
			command.stop();
			
			if (thread != null) {
				thread.interrupt();
			}
		}
	}
}
