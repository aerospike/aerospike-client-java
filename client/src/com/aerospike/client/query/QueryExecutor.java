/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.QueryPolicy;

public abstract class QueryExecutor {
	
	private final QueryPolicy policy;
	protected final Statement statement;
	private QueryThread[] threads;
	private volatile int nextThread;
	protected volatile Exception exception;
	
	public QueryExecutor(QueryPolicy policy, Statement statement) {
		this.policy = policy;
		this.policy.maxRetries = 0; // Retry policy must be one-shot for queries.
		this.statement = statement;
	}
	
	protected final void startThreads(Node[] nodes) {		
		// Initialize threads.
		threads = new QueryThread[nodes.length];

		for (int i = 0; i < nodes.length; i++) {
			QueryCommand command = createCommand(nodes[i]);
			threads[i] = new QueryThread(command);
		}
		
		// Initialize maximum number of nodes to query in parallel.
		nextThread = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.length)? threads.length : policy.maxConcurrentNodes;

		// Start threads. Use separate max because threadCompleted() may modify nextThread in parallel.
		int max = nextThread;

		for (int i = 0; i < max; i++) {
			threads[i].start();
		}
	}
	
	private final void threadCompleted() {
	   	int index = -1;
		
		// Determine if a new thread needs to be started.
		synchronized (threads) {
			if (nextThread < threads.length) {
				index = nextThread++;
			}
		}
		
		if (index >= 0) {
			// Start new thread.
			threads[index].start();
		}
		else {
			// All threads have been started. Check status.
			for (QueryThread thread : threads) {
				if (! thread.complete) {
					// Some threads have not finished. Do nothing.
					return;
				}
			}
			// All threads complete.  Tell RecordSet thread to return complete to user.
			sendCompleted();
		}
	}

	protected final void stopThreads(Exception cause) {
    	// Exception may be null, so can't synchronize on it.
    	// Use statement instead.
    	synchronized (statement) {
    	   	if (exception != null) {
    	   		return;
    	   	}
	    	exception = cause;  		
    	}
    	
		for (QueryThread thread : threads) {
			try {
				thread.stopThread();
				thread.interrupt();
			}
			catch (Exception e) {
			}
		}
		sendCompleted();
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

	private final class QueryThread extends Thread {
		// It's ok to construct QueryCommand in another thread,
		// because QueryCommand no longer uses thread local data.
		private final QueryCommand command;
		private boolean complete;

		public QueryThread(QueryCommand command) {
			this.command = command;
		}

		public void run() {
			try {
				command.query(policy, statement);
			}
			catch (Exception e) {
				// Terminate other query threads.
				stopThreads(e);
			}			
			complete = true;
			
		   	if (exception == null) {
				threadCompleted();
		   	}
		}

		public void stopThread() {
			command.stop();
		}
	}
	
	protected abstract QueryCommand createCommand(Node node);
	protected abstract void sendCompleted();
}
