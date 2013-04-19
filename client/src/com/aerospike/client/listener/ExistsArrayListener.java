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
 * The result is sent in a single array.
 */
public interface ExistsArrayListener {
	/**
	 * This method is called when an asynchronous batch exists command completes successfully.
	 * The returned boolean array is in positional order with the original key array order.
	 * 
	 * @param keys				unique record identifiers
	 * @param exists			whether keys exists on server
	 */
	public void onSuccess(Key[] keys, boolean[] exists);
	
	/**
	 * This method is called when an asynchronous exists command fails.
	 * 
	 * @param exception			error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
