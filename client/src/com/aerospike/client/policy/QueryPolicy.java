/*
 * Copyright 2012-2018 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
	
	/**
	 * Indicates if bin data is retrieved. If false, only record digests (and user keys
	 * if stored on the server) are retrieved.
	 */
	public boolean includeBinData = true;

	/**
	 * Terminate query if cluster is in migration state.
	 */
	public boolean failOnClusterChange;

	/**
	 * Copy query policy from another query policy.
	 */
	public QueryPolicy(QueryPolicy other) {
		super(other);
		this.maxConcurrentNodes = other.maxConcurrentNodes;
		this.recordQueueSize = other.recordQueueSize;
		this.includeBinData = other.includeBinData;
		this.failOnClusterChange = other.failOnClusterChange;
	}

	/**
	 * Default constructor.
	 */
	public QueryPolicy() {
		// Queries should not retry.
		super.maxRetries = 0;
		super.socketTimeout = 30000;
	}
}
