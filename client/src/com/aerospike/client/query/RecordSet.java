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
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;

/**
 * This class manages record retrieval from queries.
 * Multiple threads will retrieve records from the server nodes and put these records on the queue.
 * The single user thread consumes these records from the queue.
 */
public final class RecordSet implements Closeable {
	public static final KeyRecord END = new KeyRecord(null, null);
	
	private final QueryExecutor executor;
	private final BlockingQueue<KeyRecord> queue;
	private KeyRecord record;
	private volatile boolean valid = true;

	/**
	 * Initialize record set with underlying producer/consumer queue.
	 */
	protected RecordSet(QueryExecutor executor, int capacity) {
		this.executor = executor;
		this.queue = new ArrayBlockingQueue<KeyRecord>(capacity);
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
		if (! valid) {
			executor.checkForException();
			return false;
		}

		try {
			record = queue.take();
		}
		catch (InterruptedException ie) {
			valid = false;
			
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + executor.statement.taskId + " take interrupted");
			}
			return false;
		}

		if (record == END) {
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
		
		// Check if more records are available.
		if (record != END && queue.poll() != END) {
			// Some query threads may still be running. Stop these threads.
			executor.stopThreads(new AerospikeException.QueryTerminated());
		}
	}
	
	//-------------------------------------------------------
	// Meta-data retrieval methods
	//-------------------------------------------------------
	
	/**
	 * Get record's unique identifier.
	 */
	public final Key getKey() {
		return record.key;
	}
	
	/**
	 * Get record's header and bin data.
	 */
	public final Record getRecord() {
		return record.record;
	}
		
	//-------------------------------------------------------
	// Methods for internal use only.
	//-------------------------------------------------------
	
	/**
	 * Put a record on the queue.
	 */
	protected final boolean put(KeyRecord record) {
		if (! valid) {
			return false;
		}

		try {
			// This put will block if queue capacity is reached.
			queue.put(record);
			return true;
		}
		catch (InterruptedException ie) {
			if (Log.debugEnabled()) {
				Log.debug("RecordSet " + executor.statement.taskId + " put interrupted");
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
					Log.debug("RecordSet " + executor.statement.taskId + " both offer and poll failed on abort");
				}
				break;				
			}
		}
	}	
}
