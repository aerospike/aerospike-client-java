/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
package com.aerospike.client.query;

import java.util.concurrent.ExecutorService;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.QueryPolicy;

public abstract class QueryExecutor {
	
	protected final QueryPolicy policy;
	protected final Statement statement;
	private final Node[] nodes;
	protected final ExecutorService threadPool;
	private final QueryThread[] threads;
	protected volatile Exception exception;
	private int nextThread;
	
	public QueryExecutor(Cluster cluster, QueryPolicy policy, Statement statement) throws AerospikeException {
		this.policy = policy;
		this.policy.maxRetries = 0; // Retry policy must be one-shot for queries.
		this.statement = statement;
		
		this.nodes = cluster.getNodes();

		if (this.nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.");
		}

		this.threadPool = cluster.getThreadPool();
		this.threads = new QueryThread[nodes.length];
	}
	
	protected final void startThreads() {		
		// Initialize threads.
		for (int i = 0; i < nodes.length; i++) {
			QueryCommand command = createCommand(nodes[i]);
			threads[i] = new QueryThread(command);
		}

		// Initialize maximum number of nodes to query in parallel.
		nextThread = (policy.maxConcurrentNodes == 0 || policy.maxConcurrentNodes >= threads.length)? threads.length : policy.maxConcurrentNodes;

		// Start threads. Use separate max because threadCompleted() may modify nextThread in parallel.
		int max = nextThread;

		for (int i = 0; i < max; i++) {
			threadPool.execute(threads[i]);
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
			threadPool.execute(threads[index]);
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
    	synchronized (threads) {
    	   	if (exception != null) {
    	   		return;
    	   	}
	    	exception = cause;  		
    	}
    	
		for (QueryThread thread : threads) {
			try {
				thread.stop();
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

	private final class QueryThread implements Runnable {
		private final QueryCommand command;
		private Thread thread;
		private volatile boolean complete;

		public QueryThread(QueryCommand command) {
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
				// Terminate other query threads.
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
	
	protected abstract QueryCommand createCommand(Node node);
	protected abstract void sendCompleted();
}
