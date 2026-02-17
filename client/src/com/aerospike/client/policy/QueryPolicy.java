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

import com.aerospike.client.Log;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.DynamicConfiguration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicQueryConfig;

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
	 * Copy query policy from another query policy AND override certain policy attributes if they exist in the
	 * configProvider.  Any policy overrides will not get logged.
	 */
	public QueryPolicy(QueryPolicy other, ConfigurationProvider configProvider) {
		this(other);
		updateFromConfig(configProvider, false);
	}

	/**
	 * Copy query policy from another query policy AND override certain policy attributes if they exist in the
	 * configProvider.  Any default policy overrides will get logged.
	 */
	public QueryPolicy(QueryPolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy) {
		this(other);
		updateFromConfig(configProvider, isDefaultPolicy);
	}

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

	private void updateFromConfig(ConfigurationProvider configProvider, boolean log) {
		boolean logUpdate = false;
		if (configProvider == null) {
			return;
		}
		Configuration config = configProvider.fetchConfiguration();
		if (config == null) {
			return;
		}
		DynamicConfiguration dConfig = config.getDynamicConfiguration();
		if (dConfig == null) {
			return;
		}
		DynamicQueryConfig dynQC = dConfig.getDynamicQueryConfig();
		if (dynQC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}
		if (dynQC.connectTimeout != null && this.connectTimeout != dynQC.connectTimeout.value) {
			this.connectTimeout = dynQC.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynQC.replica != null && this.replica != dynQC.replica) {
			this.replica = dynQC.replica;
			if (logUpdate) {
				Log.info("Set QueryPolicy.replica = " + this.replica);
			}
		}
		if (dynQC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynQC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynQC.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynQC.sleepMultiplier != null && this.sleepMultiplier != dynQC.sleepMultiplier.value) {
			this.sleepMultiplier = dynQC.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.sleepMultiplier = " + this.sleepMultiplier);
			}
		}
		if (dynQC.socketTimeout != null && this.socketTimeout != dynQC.socketTimeout.value) {
			this.socketTimeout = dynQC.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynQC.timeoutDelay != null && this.timeoutDelay != dynQC.timeoutDelay.value) {
			this.timeoutDelay = dynQC.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynQC.totalTimeout != null && this.totalTimeout != dynQC.totalTimeout.value) {
			this.totalTimeout = dynQC.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynQC.maxRetries != null && this.maxRetries != dynQC.maxRetries.value) {
			this.maxRetries = dynQC.maxRetries.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynQC.includeBinData != null && this.includeBinData != dynQC.includeBinData.value) {
			this.includeBinData = dynQC.includeBinData.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.includeBinData = " + this.includeBinData);
			}
		}
		if (dynQC.infoTimeout != null && this.infoTimeout != dynQC.infoTimeout.value) {
			this.infoTimeout = dynQC.infoTimeout.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.infoTimeout = " + this.infoTimeout);
			}
		}
		if (dynQC.recordQueueSize != null && this.recordQueueSize != dynQC.recordQueueSize.value) {
			this.recordQueueSize = dynQC.recordQueueSize.value;
			if (logUpdate) {
				Log.info("Set QueryPolicy.recordQueueSize = " + this.recordQueueSize);
			}
		}
		if (dynQC.expectedDuration != null && this.expectedDuration != dynQC.expectedDuration) {
			this.expectedDuration = dynQC.expectedDuration;
			if (logUpdate) {
				Log.info("Set QueryPolicy.expectedDuration = " + this.expectedDuration);
			}
		}
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
