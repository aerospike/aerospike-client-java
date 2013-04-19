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

/**
 * Asynchronous result notifications for batch exists commands.
 * The results are sent one record at a time.
 */
public interface ExistsSequenceListener {
	/**
	 * This method is called when an asynchronous batch exists result is received from the server.
	 * The receive sequence is not ordered.
	 * 
	 * @param key				unique record identifier
	 * @param exists			whether key exists on server
	 */
	public void onExists(Key key, boolean exists);
	
	/**
	 * This method is called when the asynchronous batch exists command completes.
	 */
	public void onSuccess();
	
	/**
	 * This method is called when an asynchronous batch exists command fails.
	 * 
	 * @param exception			error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
