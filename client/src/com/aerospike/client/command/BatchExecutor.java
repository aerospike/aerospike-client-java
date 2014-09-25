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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.policy.BatchPolicy;

public final class BatchExecutor {
	
	public static void execute(
		Cluster cluster,
		BatchPolicy policy,
		Key[] keys,
		boolean[] existsArray,
		Record[] records,
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		
		if (keys.length == 0) {
			return;
		}
		
		List<BatchNode> batchNodes = BatchNode.generateList(cluster, keys);

		if (policy.maxConcurrentThreads == 1) {
			// Run batch requests sequentially in same thread.
			for (BatchNode batchNode : batchNodes) {
				for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
					if (records != null) {
						BatchCommandGet command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr);
						command.execute();
					}
					else {
						BatchCommandExists command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray);
						command.execute();
					}
				}
			}
		}
		else {
			// Run batch requests in parallel in separate threads.
			BatchExecutor executor = new BatchExecutor(cluster, batchNodes.size() * 2);

			// Initialize threads.  There may be multiple threads for a single node because the
			// wire protocol only allows one namespace per command.  Multiple namespaces 
			// require multiple threads per node.
			for (BatchNode batchNode : batchNodes) {
				for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
					if (records != null) {
						executor.add(new BatchCommandGet(batchNode.node, batchNamespace, policy, keys, binNames, records, readAttr));
					}
					else {
						executor.add(new BatchCommandExists(batchNode.node, batchNamespace, policy, keys, existsArray));
					}
				}
			}
			executor.execute(policy);
		}		
	}
	
	private final ExecutorService threadPool;
	private final ArrayList<BatchThread> threads;
	private final AtomicInteger completedCount;
	private volatile Exception exception;
	private int maxConcurrentThreads;
	private boolean completed;
	
	public BatchExecutor(Cluster cluster, int capacity) {
		this.threadPool = cluster.getThreadPool();
		this.threads = new ArrayList<BatchThread>(capacity);
		this.completedCount = new AtomicInteger();
	}
	
	public void add(MultiCommand command) {
		threads.add(new BatchExecutor.BatchThread(command));	
	}
	
	public void execute(BatchPolicy policy) {	
		this.maxConcurrentThreads = (policy.maxConcurrentThreads == 0 || policy.maxConcurrentThreads >= threads.size())? 
				threads.size() : policy.maxConcurrentThreads;
		
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

		// Check if all threads completed.
		if (finished < threads.size()) {
			int nextThread = finished + maxConcurrentThreads - 1;

			// Determine if a new thread needs to be started.
			if (nextThread < threads.size()) {
				// Start new thread.
				threadPool.execute(threads.get(nextThread));
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
    	
		for (BatchThread thread : threads) {
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

	private final class BatchThread implements Runnable {
		private final MultiCommand command;
		private Thread thread;

		public BatchThread(MultiCommand command) {
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
				// Terminate other threads.
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
