/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.BatchNode.BatchNamespace;
import com.aerospike.client.policy.Policy;

public final class BatchExecutor {
	
	private final ArrayList<BatchThread> threads;
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
		// Check status of other threads.
		for (BatchThread thread : threads) {
			if (! thread.complete) {
				// Some threads have not finished. Do nothing.
				return;
			}
		}
		// All threads complete.
		notifyCompleted();
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
		private boolean complete;

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
			complete = true;
			
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
