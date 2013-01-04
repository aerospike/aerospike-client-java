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
 * Code indicating how write transactions are handled.
 */
public enum RetryPolicy {
	/**
	 * If transaction fails, retry until specified timeout is reached.
	 */
	RETRY,
	
	/**
	 * Fail immediately if transaction fails.
	 */
	ONCE
}
