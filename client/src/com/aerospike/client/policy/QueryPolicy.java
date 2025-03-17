/*
 * Copyright 2012-2025 Aerospike, Inc.
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
 * <p>
 * Inherited Policy fields {@link Policy#txn} and {@link Policy#failOnFilteredOut} are ignored
 * in query commands.
 */
public class QueryPolicy extends Policy {
	/**
	 * Expected query duration. The server treats the query in different ways depending on the expected duration.
	 * This field is ignored for aggregation queries, background queries and server versions &lt; 6.0.
	 * <p>
	 * Default: {@link QueryDuration#LONG}
	 */
	public QueryDuration expectedDuration;

	/**
	 * This field is deprecated.
	 * Use {@link com.aerospike.client.query.Statement#setMaxRecords(long)} instead.
	 * <p>
	 * Approximate number of records to return to client. This number is divided by the
	 * number of nodes involved in the query.  The actual number of records returned
	 * may be less than maxRecords if node record counts are small and unbalanced across
	 * nodes.
	 * <p>
	 * Default: 0 (do not limit record count)
	 */
	@Deprecated
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
	 * Timeout in milliseconds for "cluster-stable" info command that is run when
	 * {@link #failOnClusterChange} is true and server version is less than 6.0.
	 * <p>
	 * Default: 1000
	 */
	public int infoTimeout = 1000;

	/**
	 * Should bin data be retrieved. If false, only record digests (and user keys
	 * if stored on the server) are retrieved.
	 * <p>
	 * Default: true
	 */
	public boolean includeBinData = true;

	/**
	 * Terminate query if cluster is in migration state. This field is ignored in server
	 * versions 6.0+.
	 * <p>
	 * Default: false
	 */
	public boolean failOnClusterChange;

	/**
	 * This field is deprecated and will eventually be removed. Use {@link #expectedDuration} instead.
	 * <p>
	 * For backwards compatibility: If shortQuery is true, the query is treated as a short query and
	 * {@link #expectedDuration} is ignored. If shortQuery is false, {@link #expectedDuration} is used
	 * and defaults to {@link QueryDuration#LONG}.
	 * <p>
	 * Is query expected to return less than 100 records per node.
	 * If true, the server will optimize the query for a small record set.
	 * This field is ignored for aggregation queries, background queries
	 * and server versions &lt; 6.0.
	 * <p>
	 * Default: false
	 */
	@Deprecated
	public boolean shortQuery;

	/**
	 * Copy query policy from another query policy.
	 */
	public QueryPolicy(QueryPolicy other) {
		super(other);
		this.expectedDuration = other.expectedDuration;
		this.maxRecords = other.maxRecords;
		this.maxConcurrentNodes = other.maxConcurrentNodes;
		this.recordQueueSize = other.recordQueueSize;
		this.infoTimeout = other.infoTimeout;
		this.includeBinData = other.includeBinData;
		this.failOnClusterChange = other.failOnClusterChange;
		this.shortQuery = other.shortQuery;
	}

	/**
	 * Copy query policy from another policy.
	 */
	public QueryPolicy(Policy other) {
		super(other);
	}

	/**
	 * Default constructor. Disable totalTimeout and set maxRetries.
	 * <p>
	 * The latest servers support retries on individual data partitions.
	 * This feature is useful when a cluster is migrating and partition(s)
	 * are missed or incomplete on the first query attempt.
	 * <p>
	 * If the first query attempt misses 2 of 4096 partitions, then only
	 * those 2 partitions are retried in the next query attempt from the
	 * last key digest received for each respective partition. A higher
	 * default maxRetries is used because it's wasteful to invalidate
	 * all query results because a single partition was missed.
	 */
	public QueryPolicy() {
		super.totalTimeout = 0;
		super.maxRetries = 5;
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setExpectedDuration(QueryDuration expectedDuration) {
		this.expectedDuration = expectedDuration;
	}

	public void setMaxRecords(long maxRecords) {
		this.maxRecords = maxRecords;
	}

	public void setMaxConcurrentNodes(int maxConcurrentNodes) {
		this.maxConcurrentNodes = maxConcurrentNodes;
	}

	public void setRecordQueueSize(int recordQueueSize) {
		this.recordQueueSize = recordQueueSize;
	}

	public void setInfoTimeout(int infoTimeout) {
		this.infoTimeout = infoTimeout;
	}

	public void setIncludeBinData(boolean includeBinData) {
		this.includeBinData = includeBinData;
	}

	public void setFailOnClusterChange(boolean failOnClusterChange) {
		this.failOnClusterChange = failOnClusterChange;
	}

	public void setShortQuery(boolean shortQuery) {
		this.shortQuery = shortQuery;
	}
}
