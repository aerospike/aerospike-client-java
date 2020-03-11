/*
 * Copyright 2012-2020 Aerospike, Inc.
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
	 * Approximate number of records to return to client. This number is divided by the
	 * number of nodes involved in the query.  The actual number of records returned
	 * may be less than maxRecords if node record counts are small and unbalanced across
	 * nodes.
	 * <p>
	 * This field is supported on server versions >= 4.9.
	 * <p>
	 * Default: 0 (do not limit record count)
	 */
	public long maxRecords;

	/**
	 * Maximum number of concurrent requests to server nodes at any point in time.
	 * If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then queries
	 * will be made to 8 nodes in parallel.  When a query completes, a new query will
	 * be issued until all 16 nodes have been queried.
	 * <p>
	 * Default: 0 (issue requests to all server nodes in parallel)
	 */
	public int maxConcurrentNodes;

	/**
	 * Number of records to place in queue before blocking.
	 * Records received from multiple server nodes will be placed in a queue.
	 * A separate thread consumes these records in parallel.
	 * If the queue is full, the producer threads will block until records are consumed.
	 * <p>
	 * Default: 5000
	 */
	public int recordQueueSize = 5000;

	/**
	 * Should bin data be retrieved. If false, only record digests (and user keys
	 * if stored on the server) are retrieved.
	 * <p>
	 * Default: true
	 */
	public boolean includeBinData = true;

	/**
	 * Terminate query if cluster is in migration state.
	 * Only used for server versions < 4.9.
	 * <p>
	 * Default: false
	 */
	public boolean failOnClusterChange;

	/**
	 * Copy query policy from another query policy.
	 */
	public QueryPolicy(QueryPolicy other) {
		super(other);
		this.maxRecords = other.maxRecords;
		this.maxConcurrentNodes = other.maxConcurrentNodes;
		this.recordQueueSize = other.recordQueueSize;
		this.includeBinData = other.includeBinData;
		this.failOnClusterChange = other.failOnClusterChange;
	}

	/**
	 * Default constructor.
	 * <p>
	 * Set maxRetries for non-aggregation queries with a null filter on
	 * server versions >= 4.9. All other queries are not retried.
	 * <p>
	 * The latest servers support retries on individual data partitions.
	 * This feature is useful when a cluster is migrating and partition(s)
	 * are missed or incomplete on the first query (with null filter) attempt.
	 * <p>
	 * If the first query attempt misses 2 of 4096 partitions, then only
	 * those 2 partitions are retried in the next query attempt from the
	 * last key digest received for each respective partition. A higher
	 * default maxRetries is used because it's wasteful to invalidate
	 * all query results because a single partition was missed.
	 */
	public QueryPolicy() {
		super.maxRetries = 5;
	}
}
