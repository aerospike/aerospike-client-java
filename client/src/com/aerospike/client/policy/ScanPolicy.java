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
 * Container object for optional parameters used in scan operations.
 */
public final class ScanPolicy extends Policy {
	/**
	 * Number of threads per node scan. 
	 */
	public int threadsPerNode = 1;

	/**
	 * Fraction of data to scan - not yet supported.
	 */
	public int scanPercent = 100;
	
	/**
	 * Issue scan requests in parallel or serially. 
	 */
	public boolean concurrentNodes = true;
	
	/**
	 * Indicates if bin data is retrieved. If false, only record digests are retrieved.
	 */
	public boolean includeBinData = true;
	
	/**
	 * Terminate scan if cluster in fluctuating state.
	 */
	public boolean failOnClusterChange;
}
