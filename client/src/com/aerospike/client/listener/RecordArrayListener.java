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
 * Asynchronous result notifications for batch get commands.
 * The result is sent in a single array.
 */
public interface RecordArrayListener {
	/**
	 * This method is called when an asynchronous batch get command completes successfully.
	 * The returned record array is in positional order with the original key array order.
	 * 
	 * @param keys			unique record identifiers
	 * @param records		record instances, an instance will be null if the key is not found
	 */
	public void onSuccess(Key[] keys, Record[] records);
	
	/**
	 * This method is called when an asynchronous batch get command fails.
	 * 
	 * @param exception		error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
