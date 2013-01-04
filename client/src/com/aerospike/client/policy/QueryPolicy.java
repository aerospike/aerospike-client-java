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
 * Container object for policy attributes used in query operations.
 */
public class QueryPolicy extends Policy {
	/**
	 * Number of threads per node scan. 
	 */
	public int threadsPerNode = 1;

	/**
	 * Issue scan requests in parallel or serially. 
	 */
	public boolean concurrentNodes = true;
	
	/**
	 * Terminate scan if cluster in fluctuating state.
	 */
	public boolean failOnClusterChange;
}
