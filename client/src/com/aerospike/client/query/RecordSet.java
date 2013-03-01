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
public abstract class RecordSet {
	private final BlockingQueue<KeyRecord> queue;
	private KeyRecord record;
	private volatile boolean valid;

	/**
	 * Initialize record set with underlying producer/consumer queue.
	 */
	protected RecordSet(int capacity) {
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
				
				if (record == null) {
					checkForException();
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
				valid = false;
			}
		}
		return valid;
	}
	
	protected abstract void checkForException() throws AerospikeException;
}
