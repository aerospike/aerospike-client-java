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
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicScanConfig;

/**
 * Container object for optional parameters used in scan operations.
 * <p>
 * Inherited Policy fields {@link Policy#txn} and {@link Policy#failOnFilteredOut} are ignored in
 * scan commands.
 */
public final class ScanPolicy extends Policy {
	/**
	 * Approximate number of records to return to client. This number is divided by the
	 * number of nodes involved in the scan.  The actual number of records returned
	 * may be less than maxRecords if node record counts are small and unbalanced across
	 * nodes.
	 * <p>
	 * Default: 0 (do not limit record count)
	 */
	public long maxRecords;

	/**
	 * Limit returned records per second (rps) rate for each server.
	 * Do not apply rps limit if recordsPerSecond is zero.
	 * <p>
	 * Default: 0
	 */
	public int recordsPerSecond;

	/**
	 * Maximum number of concurrent requests to server nodes at any point in time.
	 * If there are 16 nodes in the cluster and maxConcurrentNodes is 8, then scan requests
	 * will be made to 8 nodes in parallel.  When a scan completes, a new scan request will
	 * be issued until all 16 nodes have been scanned.
	 * <p>
	 * This field is only relevant when concurrentNodes is true.
	 * <p>
	 * Default: 0 (issue requests to all server nodes in parallel)
	 */
	public int maxConcurrentNodes;

	/**
	 * Should scan requests be issued in parallel.
	 * <p>
	 * Default: true
	 */
	public boolean concurrentNodes = true;

	/**
	 * Should bin data be retrieved. If false, only record digests (and user keys
	 * if stored on the server) are retrieved.
	 * <p>
	 * Default: true
	 */
	public boolean includeBinData = true;

	/**
	 * Copy scan policy from another scan policy AND override certain policy attributes if they exist in the configProvider.
	 * Any policy overrides will not get logged.
	 */
	public ScanPolicy(ScanPolicy other, ConfigurationProvider configProvider) {
		this(other);
		updateFromConfig(configProvider, false);
	}

	/**
	 * Copy scan policy from another scan policy AND override certain policy attributes if they exist in the configProvider.
	 * Any default policy overrides will get logged.
	 */
	public ScanPolicy(ScanPolicy other, ConfigurationProvider configProvider, boolean isDefaultPolicy) {
		this(other);
		updateFromConfig(configProvider, isDefaultPolicy);
	}

	/**
	 * Copy scan policy from another scan policy.
	 */
	public ScanPolicy(ScanPolicy other) {
		super(other);
		this.maxRecords = other.maxRecords;
		this.recordsPerSecond = other.recordsPerSecond;
		this.maxConcurrentNodes = other.maxConcurrentNodes;
		this.concurrentNodes = other.concurrentNodes;
		this.includeBinData = other.includeBinData;
	}

	/**
	 * Copy scan policy from another policy.
	 */
	public ScanPolicy(Policy other) {
		super(other);
	}

	/**
	 * Default constructor. Disable totalTimeout and set maxRetries.
	 * <p>
	 * The latest servers support retries on individual data partitions.
	 * This feature is useful when a cluster is migrating and partition(s)
	 * are missed or incomplete on the first scan attempt.
	 * <p>
	 * If the first scan attempt misses 2 of 4096 partitions, then only
	 * those 2 partitions are retried in the next scan attempt from the
	 * last key digest received for each respective partition. A higher
	 * default maxRetries is used because it's wasteful to invalidate
	 * all scan results because a single partition was missed.
	 */
	public ScanPolicy() {
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
		DynamicScanConfig dynSC = dConfig.getDynamicScanConfig();
		if (dynSC == null) {
			return;
		}

		if (log && Log.infoEnabled()) {
			logUpdate = true;
		}

		if (dynSC.connectTimeout != null && this.connectTimeout != dynSC.connectTimeout.value) {
			this.connectTimeout = dynSC.connectTimeout.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.connectTimeout = " + this.connectTimeout);
			}
		}
		if (dynSC.replica != null && this.replica != dynSC.replica) {
			this.replica = dynSC.replica;
			if (logUpdate) {
				Log.info("Set ScanPolicy.replica = " + this.replica);
			}
		}
		if (dynSC.sleepBetweenRetries != null && this.sleepBetweenRetries != dynSC.sleepBetweenRetries.value) {
			this.sleepBetweenRetries = dynSC.sleepBetweenRetries.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.sleepBetweenRetries = " + this.sleepBetweenRetries);
			}
		}
		if (dynSC.sleepMultiplier != null && this.sleepMultiplier != dynSC.sleepMultiplier.value) {
			this.sleepMultiplier = dynSC.sleepMultiplier.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.sleepMultiplier = " + this.sleepMultiplier);
			}
		}
		if (dynSC.socketTimeout != null && this.timeoutDelay != dynSC.timeoutDelay.value) {
			this.socketTimeout = dynSC.socketTimeout.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.socketTimeout = " + this.socketTimeout);
			}
		}
		if (dynSC.timeoutDelay != null && this.timeoutDelay != dynSC.timeoutDelay.value) {
			this.timeoutDelay = dynSC.timeoutDelay.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.timeoutDelay = " + this.timeoutDelay);
			}
		}
		if (dynSC.totalTimeout != null && this.totalTimeout != dynSC.totalTimeout.value) {
			this.totalTimeout = dynSC.totalTimeout.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.totalTimeout = " + this.totalTimeout);
			}
		}
		if (dynSC.maxRetries != null && this.maxRetries != dynSC.maxRetries.value) {
			this.maxRetries = dynSC.maxRetries.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.maxRetries = " + this.maxRetries);
			}
		}
		if (dynSC.concurrentNodes != null && this.concurrentNodes != dynSC.concurrentNodes.value) {
			this.concurrentNodes = dynSC.concurrentNodes.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.concurrentNodes = " + this.concurrentNodes);
			}
		}
		if (dynSC.maxConcurrentNodes != null && this.maxConcurrentNodes != dynSC.maxConcurrentNodes.value) {
			this.maxConcurrentNodes = dynSC.maxConcurrentNodes.value;
			if (logUpdate) {
				Log.info("Set ScanPolicy.maxConcurrentNodes = " + this.maxConcurrentNodes);
			}
		}
	}

	// Include setters to facilitate Spring's ConfigurationProperties.

	public void setMaxRecords(long maxRecords) {
		this.maxRecords = maxRecords;
	}

	public void setRecordsPerSecond(int recordsPerSecond) {
		this.recordsPerSecond = recordsPerSecond;
	}

	public void setMaxConcurrentNodes(int maxConcurrentNodes) {
		this.maxConcurrentNodes = maxConcurrentNodes;
	}

	public void setConcurrentNodes(boolean concurrentNodes) {
		this.concurrentNodes = concurrentNodes;
	}

	public void setIncludeBinData(boolean includeBinData) {
		this.includeBinData = includeBinData;
	}
}
