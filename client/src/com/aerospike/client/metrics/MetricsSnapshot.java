/*
 * Copyright 2012-2026 Aerospike, Inc.
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
package com.aerospike.client.metrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable point-in-time snapshot of client metrics.
 * <p>
 * The snapshot and all contained collections are immutable.
 */
public final class MetricsSnapshot {
	/** Point in time represented by this snapshot. */
    public final Instant timestamp;

	/** Whether metrics collection is enabled for this client. */
	public final boolean metricsEnabled;

	/** Whether operational metrics collection is enabled for this client. */
	public final boolean operationalMetricsEnabled;

	/** Whether usage metrics collection is enabled for this client. */
	public final boolean usageMetricsEnabled;

	/** Cluster name. */
    public final String clusterName;

	/** Client implementation type. */
    public final String clientType;

	/** Client library version. */
    public final String clientVersion;

	/** Application identifier resolved for this client. */
    public final String appId;

	/** Immutable application-defined labels. */
    public final Map<String, String> labels;

	/** Current number of active cluster nodes. */
	public final int totalNodes;

	/** Current number of commands in the recovery queue. */
    public final int recoverQueueSize;

	/** Current number of invalid cluster nodes. */
    public final int invalidNodeCount;

	/** Cumulative commands that timed out in the delay queue. */
    public final long delayQueueTimeoutCount;

	/** Current number of open client connections. */
    public final long openConnections;

	/** Cumulative client commands. */
	public final long commandCount;

	/** Cumulative command retries. */
	public final long commandRetries;

	/**
	 * Legacy alias for {@link #commandRetries}.
	 */
	public final long retryCount;

	/**
	 * Batch partial failure count. Remains zero until collection is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long batchPartialFailure;

	/**
	 * Legacy Java count of commands that exceeded maximum retries.
	 * Remains zero until collection is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long exceededMaxRetries;

	/**
	 * Legacy Java count of commands that exceeded total timeout.
	 * Remains zero until collection is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long exceededTotalTimeout;

	/** Current process CPU usage percentage. */
    public final double cpuPercent;

	/** Current process memory usage in bytes. */
    public final long memoryBytes;

	/** Cluster-scoped read latency histogram. */
	public final LatencySnapshot readLatency;

	/** Cluster-scoped write latency histogram. */
	public final LatencySnapshot writeLatency;

	/** Cluster-scoped batch latency histogram. */
	public final LatencySnapshot batchLatency;

	/** Cluster-scoped query latency histogram. */
	public final LatencySnapshot queryLatency;

	/**
	 * Connection validation open failures. Remains zero until collection is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long validationOpenFailure;

	/**
	 * Connection validation TLS failures. Remains zero until collection is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long validationTlsFailure;

	/**
	 * Connection validation authentication failures. Remains zero until collection is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final long validationAuthFailure;

	/** Current metrics for Java event loops. */
    public final List<EventLoopMetricsSnapshot> eventLoops;

	/** Current metrics for active cluster nodes. */
    public final List<NodeMetricsSnapshot> nodes;

	/**
	 * Metrics for departed nodes awaiting reporting. Remains empty until collection support
	 * is implemented.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final List<NodeMetricsSnapshot> nodesDeparted;

	/** Client usage counters. */
	public final UsageMetricsSnapshot usage;

	/** Unit used for latency histogram buckets. */
    public final LatencyUnit latencyUnit;

	/** Number of latency histogram columns. */
    public final int latencyColumns;

	/** Exponential shift between latency histogram columns. */
	public final int latencyShift;

	/**
	 * Initialize an immutable point-in-time metrics snapshot.
	 */
    public MetricsSnapshot(
            Instant timestamp,
			boolean metricsEnabled,
			boolean operationalMetricsEnabled,
			boolean usageMetricsEnabled,
            String clusterName,
            String clientType,
            String clientVersion,
            String appId,
            Map<String, String> labels,
            int totalNodes,
			int recoverQueueSize,
			int invalidNodeCount,
			long delayQueueTimeoutCount,
            long openConnections,
			long commandCount,
			long commandRetries,
			long batchPartialFailure,
            long exceededMaxRetries,
            long exceededTotalTimeout,
            double cpuPercent,
            long memoryBytes,
			LatencySnapshot readLatency,
			LatencySnapshot writeLatency,
			LatencySnapshot batchLatency,
			LatencySnapshot queryLatency,
			long validationOpenFailure,
			long validationTlsFailure,
			long validationAuthFailure,
            List<EventLoopMetricsSnapshot> eventLoops,
            List<NodeMetricsSnapshot> nodes,
			List<NodeMetricsSnapshot> nodesDeparted,
			UsageMetricsSnapshot usage,
            LatencyUnit latencyUnit,
            int latencyColumns,
			int latencyShift
    ) {
        this.timestamp = timestamp;
		this.metricsEnabled = metricsEnabled;
		this.operationalMetricsEnabled = operationalMetricsEnabled;
		this.usageMetricsEnabled = usageMetricsEnabled;
        this.clusterName = clusterName;
        this.clientType = clientType;
        this.clientVersion = clientVersion;
        this.appId = appId;
        this.labels = labels != null
                ? Collections.unmodifiableMap(new HashMap<>(labels))
                : Collections.emptyMap();
        this.totalNodes = totalNodes;
        this.recoverQueueSize = recoverQueueSize;
        this.invalidNodeCount = invalidNodeCount;
        this.delayQueueTimeoutCount = delayQueueTimeoutCount;
        this.openConnections = openConnections;
		this.commandCount = commandCount;
		this.commandRetries = commandRetries;
		this.retryCount = commandRetries;
		this.batchPartialFailure = batchPartialFailure;
        this.exceededMaxRetries = exceededMaxRetries;
        this.exceededTotalTimeout = exceededTotalTimeout;
        this.cpuPercent = cpuPercent;
        this.memoryBytes = memoryBytes;
		this.readLatency = readLatency;
		this.writeLatency = writeLatency;
		this.batchLatency = batchLatency;
		this.queryLatency = queryLatency;
		this.validationOpenFailure = validationOpenFailure;
		this.validationTlsFailure = validationTlsFailure;
		this.validationAuthFailure = validationAuthFailure;
        this.eventLoops = Collections.unmodifiableList(new ArrayList<>(eventLoops));
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
		this.nodesDeparted = Collections.unmodifiableList(new ArrayList<>(nodesDeparted));
		this.usage = usage;
        this.latencyUnit = latencyUnit;
        this.latencyColumns = latencyColumns;
		this.latencyShift = latencyShift;
    }

	@Override
	public String toString() {
		return "MetricsSnapshot{"
			+ "timestamp=" + timestamp
			+ ", metricsEnabled=" + metricsEnabled
			+ ", operationalMetricsEnabled=" + operationalMetricsEnabled
			+ ", usageMetricsEnabled=" + usageMetricsEnabled
			+ ", clusterName='" + clusterName + '\''
			+ ", clientType='" + clientType + '\''
			+ ", clientVersion='" + clientVersion + '\''
			+ ", appId='" + appId + '\''
			+ ", labels=" + labels
			+ ", totalNodes=" + totalNodes
			+ ", recoverQueueSize=" + recoverQueueSize
			+ ", invalidNodeCount=" + invalidNodeCount
			+ ", delayQueueTimeoutCount=" + delayQueueTimeoutCount
			+ ", openConnections=" + openConnections
			+ ", commandCount=" + commandCount
			+ ", commandRetries=" + commandRetries
			+ ", retryCount=" + retryCount
			+ ", batchPartialFailure=" + batchPartialFailure
			+ ", exceededMaxRetries=" + exceededMaxRetries
			+ ", exceededTotalTimeout=" + exceededTotalTimeout
			+ ", cpuPercent=" + cpuPercent
			+ ", memoryBytes=" + memoryBytes
			+ ", readLatency=" + readLatency
			+ ", writeLatency=" + writeLatency
			+ ", batchLatency=" + batchLatency
			+ ", queryLatency=" + queryLatency
			+ ", validationOpenFailure=" + validationOpenFailure
			+ ", validationTlsFailure=" + validationTlsFailure
			+ ", validationAuthFailure=" + validationAuthFailure
			+ ", eventLoops=" + eventLoops
			+ ", nodes=" + nodes
			+ ", nodesDeparted=" + nodesDeparted
			+ ", usage=" + usage
			+ ", latencyUnit=" + latencyUnit
			+ ", latencyColumns=" + latencyColumns
			+ ", latencyShift=" + latencyShift
			+ '}';
	}
}
