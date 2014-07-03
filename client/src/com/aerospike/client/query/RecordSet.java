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
		if (valid) {
			try {
				record = queue.take();

				if (record == END) {
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
		if (valid) {
			try {
				queue.put(record);
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
