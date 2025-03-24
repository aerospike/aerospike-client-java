/*
 * Copyright 2012-2024 Aerospike, Inc.
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

import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.dynamicconfig.DynamicQueryConfig;
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
	public boolean concurrentNodes = PolicyDefaultValues.CONCURRENT_NODES;

	/**
	 * Should bin data be retrieved. If false, only record digests (and user keys
	 * if stored on the server) are retrieved.
	 * <p>
	 * Default: true
	 */
	public boolean includeBinData = true;

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
		super.totalTimeout = PolicyDefaultValues.TOTAL_TIMEOUT_SCAN;
		super.maxRetries = PolicyDefaultValues.TOTAL_TIMEOUT_SCAN;
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

	/**
	 * Override certain policy attributes if they exist in the configProvider.
	 */
	public void applyConfigOverrides( ConfigurationProvider configProvider) {
		Configuration config = configProvider.fetchConfiguration();
		DynamicScanConfig dynSC = config.dynamicConfiguration.dynamicScanConfig;

		if (dynSC.readModeAP != null) this.readModeAP = dynSC.readModeAP;
		if (dynSC.readModeSC != null) this.readModeSC = dynSC.readModeSC;
		if (dynSC.connectTimeout != null) this.connectTimeout = dynSC.connectTimeout.value;
		if (dynSC.replica != null) this.replica = dynSC.replica;
		if (dynSC.sleepBetweenRetries != null) this.sleepBetweenRetries = dynSC.sleepBetweenRetries.value;
		if (dynSC.socketTimeout != null) this.socketTimeout = dynSC.socketTimeout.value;
		if (dynSC.timeoutDelay != null) this.timeoutDelay = dynSC.timeoutDelay.value;
		if (dynSC.totalTimeout != null) this.totalTimeout = dynSC.totalTimeout.value;
		if (dynSC.maxRetries != null) this.maxRetries = dynSC.maxRetries.value;
		if (dynSC.concurrentNodes != null) this.concurrentNodes = dynSC.concurrentNodes.value;
		if (dynSC.maxConcurrentNodes != null) this.maxConcurrentNodes = dynSC.maxConcurrentNodes.value;
	}
}
