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
 * Asynchronous result notifications for exists commands.
 */
public interface ExistsListener {
	/**
	 * This method is called when an asynchronous exists command completes successfully.
	 * 
	 * @param key				unique record identifier
	 * @param exists			whether key exists on server
	 */
	public void onSuccess(Key key, boolean exists);
	
	/**
	 * This method is called when an asynchronous exists command fails.
	 * 
	 * @param exception			error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
