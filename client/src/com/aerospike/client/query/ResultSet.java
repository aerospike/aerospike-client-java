/*
 * Copyright 2012-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
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
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;

/**
 * Iterable stream of aggregation results from a query that uses an aggregation UDF.
 *
 * <p>Producer threads fill an internal queue with results from the server; the consumer thread calls
 * {@link #next()} to block until the next result is available, then {@link #getObject()} to access it.
 * Call {@link #close()} when done to release resources and optionally terminate the query early.
 *
 * <p>Call queryAggregate then iterate with next() and getObject(); close the ResultSet when done.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * ResultSet rs = client.queryAggregate(queryPolicy, stmt);
 * try {
 *     while (rs.next()) {
 *         Object result = rs.getObject();
 *         // process aggregation result
 *     }
 * } finally {
 *     rs.close();
 * }
 * }</pre>
 *
 * @see RecordSet for query results that return full records
 */
public class ResultSet implements Iterable<Object>, Closeable {
	/** Sentinel value placed on the queue when no more results are available. */
	public static final Object END = new Object();

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

	/**
	 * For internal use only.
	 */
	protected ResultSet() {
		this.executor = null;
		this.queue = null;
	}

	//-------------------------------------------------------
	// Result traversal methods
	//-------------------------------------------------------

	/**
	 * Advances to the next result, blocking until one is available or the query ends.
	 *
	 * @return {@code true} if a result is available (use {@link #getObject()}), {@code false} if no more results or query was closed
	 * @throws AerospikeException	when the query failed on the server (e.g. timeout, connection error, or invalid statement).
	 */
	public boolean next() throws AerospikeException {
		if (!valid) {
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
	 * Closes this result set and releases resources; may signal the server to stop the query.
	 *
	 * <p>Call in a {@code finally} block to ensure resources are released. After close, {@link #next()} returns {@code false}.
	 */
	public void close() {
		valid = false;

		// Check if more results are available.
		if (row != END && queue.poll() != END) {
			// Some query threads may still be running. Stop these threads.
			executor.stopThreads(new AerospikeException.QueryTerminated());
		}
	}

	/**
	 * Returns an iterator over the results in this set.
	 *
	 * @return an iterator over the aggregation result objects
	 */
	@Override
	public Iterator<Object> iterator() {
		return new ResultSetIterator(this);
	}

	//-------------------------------------------------------
	// Meta-data retrieval methods
	//-------------------------------------------------------

	/**
	 * Returns the current aggregation result (valid after {@link #next()} returns {@code true}).
	 *
	 * @return the current result object (type depends on the UDF)
	 */
	public Object getObject() {
		return row;
	}

	//-------------------------------------------------------
	// Methods for internal use only.
	//-------------------------------------------------------

	/**
	 * Put object on the queue.
	 */
	public boolean put(Object object) {
		if (!valid) {
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
	protected void abort() {
		valid = false;
		queue.clear();

		// Send end command to command thread.
		// It's critical that the end offer succeeds.
		while (!queue.offer(END)) {
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

	/**
	 * Support standard iteration interface for RecordSet.
	 */
	private class ResultSetIterator implements Iterator<Object>, Closeable {

		private final ResultSet resultSet;
		private boolean more;

		ResultSetIterator(ResultSet resultSet) {
			this.resultSet = resultSet;
			more = this.resultSet.next();
		}

		@Override
		public boolean hasNext() {
			return more;
		}

		@Override
		public Object next() {
			Object obj = resultSet.row;
			more = resultSet.next();
			return obj;
		}

		@Override
		public void remove() {
		}

		@Override
		public void close() {
			resultSet.close();
		}
	}
}
