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
	 * Maximum number of concurrent requests to server nodes at any point in time.
	 * If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then queries 
	 * will be made to 8 nodes in parallel.  When a query completes, a new query will 
	 * be issued until all 16 nodes have been queried.
	 * Default (0) is to issue requests to all server nodes in parallel.
	 */
	public int maxConcurrentNodes;
	
	/**
	 * Number of records to place in queue before blocking.
	 * Records received from multiple server nodes will be placed in a queue.
	 * A separate thread consumes these records in parallel.
	 * If the queue is full, the producer threads will block until records are consumed.
	 */
	public int recordQueueSize = 5000;
}
