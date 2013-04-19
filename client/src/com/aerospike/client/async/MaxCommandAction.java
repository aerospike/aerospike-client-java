/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.async;

/**
 * How to handle cases when the asynchronous maximum number of concurrent database commands have been exceeded.
 */
public enum MaxCommandAction {
	/**
	 * Accept and process command.  This implies the user is responsible for throttling asynchronous load. 
	 */
	ACCEPT,

	/**
	 * Reject database command.
	 */
	REJECT,

	/**
	 * Block until a previous command completes. 
	 */
	BLOCK,
}
