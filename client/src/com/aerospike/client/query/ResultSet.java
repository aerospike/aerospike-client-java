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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.aerospike.client.AerospikeException;

/**
 * This class manages record retrieval from queries.
 * Multiple threads will retrieve records from the server nodes and put these records on the queue.
 * The single user thread consumes these records from the queue.
 */
public final class ResultSet {
	public static final Integer END = new Integer(-1);
	
	private final QueryAggregateExecutor executor;
	private final BlockingQueue<Object> queue;
	private Object row;
	private volatile boolean valid = true;

	/**
	 * Initialize record set with underlying producer/consumer queue.
	 */
	protected ResultSet(QueryAggregateExecutor executor, int capacity) {
		this.executor = executor;
		this.queue = new ArrayBlockingQueue<Object>(capacity);
	}
	
	//-------------------------------------------------------
	// Record traversal methods
	//-------------------------------------------------------

	/**
	 * Retrieve next record.  This method will block until a record is retrieved 
	 * or the query is cancelled.
	 * 
	 * @return		whether record exists - if false, no more records are available 
	 */
	public final boolean next() throws AerospikeException {
		if (valid) {
			try {
				row = queue.take();

				if (row == END) {
					executor.checkForException();
					valid = false;
				}
			}
			catch (InterruptedException ie) {
				valid = false;
			}
		}
		return valid;
	}
	
	/**
	 * Cancel query.
	 */
	public final void close() {
		valid = false;
	}
	
	//-------------------------------------------------------
	// Meta-data retrieval methods
	//-------------------------------------------------------
		
	/**
	 * Get record's header and bin data.
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
		if (valid) {
			try {
				queue.put(object);
			}
			catch (InterruptedException ie) {
				abort();
			}
		}
		return valid;
	}
	
	/**
	 * Abort retrieval with end token.
	 */
	private final void abort() {
		valid = false;
		
		// It's critical that the end put succeeds.
		// Loop through all interrupts.
		while (true) {
			try {
				queue.put(END);
				return;
			}
			catch (InterruptedException ie) {
			}
		}
	}
}
