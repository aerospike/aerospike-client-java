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

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.aerospike.client.policy.Priority;

/**
 * Asynchronous task handle for database requests that do not return data.
 */
public class ExecuteTask extends FutureTask<Void> {
	/**
	 * Asynchronous return status callback definition. 
	 */
	public interface Callback {	
		public void taskCompleted(ExecuteTask task);
	}

	/**
	 * Asynchronous return status callback. 
	 */
	public final Callback callback;
	
	/**
	 * Task ID.
	 */
	public final int id;
	
	/**
	 * Initialize asynchronous task. 
	 */
	public ExecuteTask(int id, Callable<Void> callable, Callback callback) {
		super(callable);
		this.id = id;
		this.callback = callback;
	}

	/**
	 * Run task at different priority. Not implemented.
	 */
	public void reprioritize(Priority priority) {
	}	
}
