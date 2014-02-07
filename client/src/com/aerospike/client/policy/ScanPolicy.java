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
	 * Fraction of data to scan - not yet supported.
	 */
	public int scanPercent = 100;
	
	/**
	 * Maximum number of concurrent requests to server nodes at any point in time.
	 * If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then scan requests
	 * will be made to 8 nodes in parallel.  When a scan completes, a new scan request will 
	 * be issued until all 16 nodes have been scanned.
	 * <p>
	 * This field is only relevant when concurrentNodes is true.
	 * Default (0) is to issue requests to all server nodes in parallel.
	 */
	public int maxConcurrentNodes;

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
