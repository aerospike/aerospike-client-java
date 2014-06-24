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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.policy.Policy;

public final class BatchExecutor {
	
	private final ArrayList<BatchThread> threads;
	private final AtomicInteger completedCount;
	private volatile Exception exception;
	private boolean completed;
	
	public BatchExecutor(
		Cluster cluster,
		Policy policy, 
		Key[] keys,
		boolean[] existsArray, 
		Record[] records, 
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		
		completedCount = new AtomicInteger();
		List<BatchNode> batchNodes = BatchNode.generateList(cluster, keys);
		HashMap<Key,BatchItem> keyMap = BatchItem.generateMap(keys);
		
		// Initialize threads.  There may be multiple threads for a single node because the
		// wire protocol only allows one namespace per command.  Multiple namespaces 
		// require multiple threads per node.
		threads = new ArrayList<BatchThread>(batchNodes.size() * 2);
		MultiCommand command = null;

		for (BatchNode batchNode : batchNodes) {
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				if (records != null) {
					command = new BatchCommandGet(batchNode.node, batchNamespace, policy, keyMap, binNames, records, readAttr);
				}
				else {
					command = new BatchCommandExists(batchNode.node, batchNamespace, policy, keyMap, existsArray);
				}
				threads.add(new BatchThread(command));
			}
		}
		
		ExecutorService threadPool = cluster.getThreadPool();
		
		for (BatchThread thread : threads) {
			threadPool.execute(thread);
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
		// Check if all threads completed.
		if (completedCount.incrementAndGet() >= threads.size()) {
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
		private MultiCommand command;
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
