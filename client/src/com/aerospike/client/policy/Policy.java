/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.policy;


/**
 * Container object for transaction policy attributes used in all database
 * operation calls.
 */
public class Policy {	
	/**
	 * Action taken when a transaction fails.
	 */
	public RetryPolicy retryPolicy = RetryPolicy.RETRY;
	
	/**
	 * Priority of request relative to other requests.
	 */
	public Priority priority = Priority.DEFAULT;
	
	/**
	 * Transaction timeout in milliseconds.  Default to no timeout (0).
	 */
	public int timeout;
}
