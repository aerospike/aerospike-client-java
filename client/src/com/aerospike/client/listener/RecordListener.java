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
 * Asynchronous result notifications for get or operate commands.
 */
public interface RecordListener {
	/**
	 * This method is called when an asynchronous get or operate command completes successfully.
	 * 
	 * @param key			unique record identifier
	 * @param record		record instance if found, otherwise null
	 */
	public void onSuccess(Key key, Record record);

	/**
	 * This method is called when an asynchronous get or operate command fails.
	 * 
	 * @param exception		error that occurred
	 */
	public void onFailure(AerospikeException exception);
}
