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

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;

/**
 * This class manages result retrieval from queries.
 * Multiple threads will retrieve results from the server nodes and put these results on the queue.
 * The single user thread consumes these results from the queue.
 */
public final class ResultSet implements Closeable {
	public static final Integer END = new Integer(-1);
	
	private final QueryAggregateExecutor executor;
	private final BlockingQueue<Object> queue;
	private Object row;
	private volatile boolean valid = true;

	/**
	 * Initialize result set with underlying producer/consumer queue.
	 */
	protected ResultSet(QueryAggregateExecutor executor, int capacity) {
		this.executor = executor;
		this.queue = new ArrayBlockingQueue<Object>(capacity);
	}
	
	//-------------------------------------------------------
	// Result traversal methods
	//-------------------------------------------------------

	/**
	 * Retrieve next result.  This method will block until a result is retrieved 
	 * or the query is cancelled.
	 * 
	 * @return	whether result exists - if false, no more results are available 
	 */
	public final boolean next() throws AerospikeException {
		if (! valid) {
			executor.checkForException();
			return false;
		}

		try {
			row = queue.take();
		}
		catch (InterruptedException ie) {
			valid = false;
			
			if (Log.debugEnabled()) {
				Log.debug("ResultSet " + executor.statement.taskId + " take interrupted");
			}
			return false;
		}

		if (row == END) {
			valid = false;
			executor.checkForException();
			return false;
		}
		return true;
	}
	
	/**
	 * Close query.
	 */
	public final void close() {
		valid = false;

		// Check if more results are available.
		if (row != END && queue.poll() != END) {
			// Some query threads may still be running. Stop these threads.
			executor.stopThreads(new AerospikeException.QueryTerminated());
		}
	}
	
	//-------------------------------------------------------
	// Meta-data retrieval methods
	//-------------------------------------------------------
		
	/**
	 * Get result.
	 */
	public final Object getObject() {
		return row;
	}
		
	//-------------------------------------------------------
	// Methods for internal use only.
	//-------------------------------------------------------
	
	/**
	 * Put object on the queue.
	 */
	public final boolean put(Object object) {
		if (! valid) {
			return false;
		}

		try {
			// This put will block if queue capacity is reached.
			queue.put(object);
			return true;
		}
		catch (InterruptedException ie) {
			if (Log.debugEnabled()) {
				Log.debug("ResultSet " + executor.statement.taskId + " put interrupted");
			}

			// Valid may have changed.  Check again.
			if (valid) {
				abort();
			}
			return false;
		}
	}
	
	/**
	 * Abort retrieval with end token.
	 */
	protected final void abort() {
		valid = false;
		queue.clear();
		
		// Send end command to transaction thread.
		// It's critical that the end offer succeeds.
		while (! queue.offer(END)) {
			// Queue must be full. Remove one item to make room.
			if (queue.poll() == null) {
				// Can't offer or poll.  Nothing further can be done.
				if (Log.debugEnabled()) {
					Log.debug("ResultSet " + executor.statement.taskId + " both offer and poll failed on abort");
				}
				break;				
			}
		}
	}
}
