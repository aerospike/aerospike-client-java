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
	 * Priority of request relative to other transactions.
	 * Currently, only used for scans.
	 */
	public Priority priority = Priority.DEFAULT;
	
	/**
	 * Transaction timeout in milliseconds.
	 * This timeout is used to set the socket timeout and is also sent to the 
	 * server along with the transaction in the wire protocol.
	 * Default to no timeout (0).
	 */
	public int timeout;
	
	/**
	 * Maximum number of retries before aborting the current transaction.
	 * A retry is attempted when there is a network error other than timeout.  
	 * If maxRetries is exceeded, the abort will occur even if the timeout 
	 * has not yet been exceeded.
	 */
	public int maxRetries = 2;

	/**
	 * Milliseconds to sleep between retries if a transaction fails and the 
	 * timeout was not exceeded.
	 */
	public int sleepBetweenRetries = 500;
}
