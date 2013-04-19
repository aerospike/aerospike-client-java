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
 * Asynchronous result notifications for put, append, prepend, add, delete and touch commands.
 */
public interface WriteListener {
	/**
	 * This method is called when an asynchronous write command completes successfully.
	 * 
	 * @param key					unique record identifier
	 */
	public void onSuccess(Key key);
	
	/**
	 * This method is called when an asynchronous write command fails.
	 * 
	 * @param exception				error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
