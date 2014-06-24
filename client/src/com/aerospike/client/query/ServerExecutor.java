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
package com.aerospike.client.query;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;

public final class ServerExecutor {
	
	private final ServerThread[] threads;
	private final AtomicInteger completedCount;
	private volatile Exception exception;
	private boolean completed;
	
	public ServerExecutor(
		Cluster cluster,
		Policy policy,
		Statement statement,
		String packageName, 
		String functionName, 
		Value[] functionArgs
	) throws AerospikeException {
		statement.setAggregateFunction(packageName, functionName, functionArgs, false);
		
		if (statement.taskId == 0) {
			Random r = new Random();
			statement.taskId = r.nextInt(Integer.MAX_VALUE);
		}
		
		completedCount = new AtomicInteger();

		Node[] nodes = cluster.getNodes();
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}

		threads = new ServerThread[nodes.length];
		
		for (int i = 0; i < nodes.length; i++) {
			ServerCommand command = new ServerCommand(nodes[i], policy, statement);
			threads[i] = new ServerThread(command);
		}
		
		ExecutorService threadPool = cluster.getThreadPool();

		for (int i = 0; i < nodes.length; i++) {
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
		// Check if all threads completed.
		if (completedCount.incrementAndGet() >= threads.length) {
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
    	
		for (ServerThread thread : threads) {
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

	private final class ServerThread implements Runnable {
		private final ServerCommand command;
		private Thread thread;

		public ServerThread(ServerCommand command) {
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
