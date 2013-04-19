/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Asynchronous result notifications for batch get and scan commands.
 * The results are sent one record at a time.
 */
public interface RecordSequenceListener {
	/**
	 * This method is called when an asynchronous record is received from the server.
	 * The receive sequence is not ordered.
	 * <p>
	 * The user may throw a 
	 * {@link com.aerospike.client.AerospikeException.QueryTerminated AerospikeException.QueryTerminated} 
	 * exception if the command should be aborted.  If any exception is thrown, parallel command threads
	 * to other nodes will also be terminated and the exception will be propagated back through the
	 * commandFailed() call.
	 * 
	 * @param key					unique record identifier
	 * @param record				record instance, will be null if the key is not found
	 * @throws AerospikeException	if error occurs or scan should be terminated.
	 */
	public void onRecord(Key key, Record record) throws AerospikeException;
	
	/**
	 * This method is called when the asynchronous batch get or scan command completes.
	 */
	public void onSuccess();
	
	/**
	 * This method is called when an asynchronous batch get or scan command fails.
	 * 
	 * @param exception				error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
