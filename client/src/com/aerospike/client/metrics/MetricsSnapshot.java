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
 * Standard metrics are exact counters or current gauges collected with negligible
 * overhead. Extended metrics (CPU, memory, command count, per-namespace counters,
 * latency histograms) may carry a performance cost and are only populated when
 * {@link #extendedMetricsEnabled} is {@code true}.
 */
public final class MetricsSnapshot {

	// ── Snapshot information ───────────────────────────────────────

	public final Instant timestamp;
	public final boolean extendedMetricsEnabled;

	// ── Identifying metadata ───────────────────────────────────────

	public final String clusterName;
	public final String clientType;
	public final String clientVersion;
	public final String appId;
	public final Map<String, String> labels;

	// ── Standard cluster-level gauges and cumulative counters ──────

	public final int recoverQueueSize;
	public final int invalidNodeCount;
	public final long retryCount;
	public final long delayQueueTimeoutCount;
	public final int totalNodes;
	public final long openConnections;
	public final long exceededMaxRetries;
	public final long exceededTotalTimeout;

	// ── Extended cluster-level metrics ──────────────────────────────

	public final double cpuPercent;
	public final long memoryBytes;
	public final long commandCount;

	// ── Event loop snapshots (Java-specific) ───────────────────────

	public final List<EventLoopSnapshot> eventLoops;

	// ── Per-node and cluster-aggregated snapshots ──────────────────

	public final List<NodeSnapshot> nodes;
	public final NodeSnapshot clusterAggregated;

	// ── Histogram configuration ────────────────────────────────────

	public final HistogramType histogramType;
	public final LatencyUnit latencyUnit;
	public final int latencyBase;
	public final int latencyColumns;

	public MetricsSnapshot(
		Instant timestamp,
		boolean extendedMetricsEnabled,
		String clusterName,
		String clientType,
		String clientVersion,
		String appId,
		Map<String, String> labels,
		int recoverQueueSize,
		int invalidNodeCount,
		long retryCount,
		long delayQueueTimeoutCount,
		int totalNodes,
		long openConnections,
		long exceededMaxRetries,
		long exceededTotalTimeout,
		double cpuPercent,
		long memoryBytes,
		long commandCount,
		List<EventLoopSnapshot> eventLoops,
		List<NodeSnapshot> nodes,
		NodeSnapshot clusterAggregated,
		HistogramType histogramType,
		LatencyUnit latencyUnit,
		int latencyBase,
		int latencyColumns
	) {
		this.timestamp = timestamp;
		this.extendedMetricsEnabled = extendedMetricsEnabled;
		this.clusterName = clusterName;
		this.clientType = clientType;
		this.clientVersion = clientVersion;
		this.appId = appId;
		this.labels = labels != null
			? Collections.unmodifiableMap(new HashMap<>(labels))
			: Collections.emptyMap();
		this.recoverQueueSize = recoverQueueSize;
		this.invalidNodeCount = invalidNodeCount;
		this.retryCount = retryCount;
		this.delayQueueTimeoutCount = delayQueueTimeoutCount;
		this.totalNodes = totalNodes;
		this.openConnections = openConnections;
		this.exceededMaxRetries = exceededMaxRetries;
		this.exceededTotalTimeout = exceededTotalTimeout;
		this.cpuPercent = cpuPercent;
		this.memoryBytes = memoryBytes;
		this.commandCount = commandCount;
		this.eventLoops = Collections.unmodifiableList(new ArrayList<>(eventLoops));
		this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
		this.clusterAggregated = clusterAggregated;
		this.histogramType = histogramType;
		this.latencyUnit = latencyUnit;
		this.latencyBase = latencyBase;
		this.latencyColumns = latencyColumns;
	}

	// ── Inner classes ──────────────────────────────────────────────

	/**
	 * Metrics snapshot for a single cluster node. Counter fields are cumulative
	 * unless their names describe a current gauge.
	 */
	public static final class NodeSnapshot {
		public final String nodeName;
		public final String nodeAddress;
		public final int nodePort;

		// Standard connection pool metrics.
		public final ConnectionSnapshot syncConnections;
		public final ConnectionSnapshot asyncConnections;

		// Standard connection lifecycle counters and gauges.
		public final long connectionAttempts;
		public final long connectionsSuccessful;
		public final long connectionsFailed;
		public final long connectionTimeoutErrors;
		public final long connectionOtherErrors;
		public final long circuitBreakerHits;
		public final long connectionPoolEmptyCount;
		public final long connectionPoolOverflowCount;
		public final long idleConnectionsDropped;
		public final long openConnections;
		public final long closedConnections;
		public final long recoveredConnections;

		// Standard cluster-maintenance counters.
		public final long tendsTotal;
		public final long tendsSuccessful;
		public final long tendsFailed;
		public final long partitionMapUpdates;
		public final long nodesAdded;
		public final long nodesRemoved;

		// Standard transaction counters.
		public final long transactionRetryCount;
		public final long transactionErrorCount;

		// Extended: per-command-type latency histograms (sampled).
		public final Map<CommandType, HistogramSnapshot> commandLatencies;

		// Extended: per-namespace metrics.
		public final List<NamespaceSnapshot> namespaces;

		public NodeSnapshot(
			String nodeName,
			String nodeAddress,
			int nodePort,
			ConnectionSnapshot syncConnections,
			ConnectionSnapshot asyncConnections,
			long connectionAttempts,
			long connectionsSuccessful,
			long connectionsFailed,
			long connectionTimeoutErrors,
			long connectionOtherErrors,
			long circuitBreakerHits,
			long connectionPoolEmptyCount,
			long connectionPoolOverflowCount,
			long idleConnectionsDropped,
			long openConnections,
			long closedConnections,
			long recoveredConnections,
			long tendsTotal,
			long tendsSuccessful,
			long tendsFailed,
			long partitionMapUpdates,
			long nodesAdded,
			long nodesRemoved,
			long transactionRetryCount,
			long transactionErrorCount,
			Map<CommandType, HistogramSnapshot> commandLatencies,
			List<NamespaceSnapshot> namespaces
		) {
			this.nodeName = nodeName;
			this.nodeAddress = nodeAddress;
			this.nodePort = nodePort;
			this.syncConnections = syncConnections;
			this.asyncConnections = asyncConnections;
			this.connectionAttempts = connectionAttempts;
			this.connectionsSuccessful = connectionsSuccessful;
			this.connectionsFailed = connectionsFailed;
			this.connectionTimeoutErrors = connectionTimeoutErrors;
			this.connectionOtherErrors = connectionOtherErrors;
			this.circuitBreakerHits = circuitBreakerHits;
			this.connectionPoolEmptyCount = connectionPoolEmptyCount;
			this.connectionPoolOverflowCount = connectionPoolOverflowCount;
			this.idleConnectionsDropped = idleConnectionsDropped;
			this.openConnections = openConnections;
			this.closedConnections = closedConnections;
			this.recoveredConnections = recoveredConnections;
			this.tendsTotal = tendsTotal;
			this.tendsSuccessful = tendsSuccessful;
			this.tendsFailed = tendsFailed;
			this.partitionMapUpdates = partitionMapUpdates;
			this.nodesAdded = nodesAdded;
			this.nodesRemoved = nodesRemoved;
			this.transactionRetryCount = transactionRetryCount;
			this.transactionErrorCount = transactionErrorCount;
			this.commandLatencies = Collections.unmodifiableMap(new HashMap<>(commandLatencies));
			this.namespaces = Collections.unmodifiableList(new ArrayList<>(namespaces));
		}
	}

	/**
	 * Standard connection pool statistics.
	 */
	public static final class ConnectionSnapshot {
		public final int inUse;
		public final int inPool;
		public final int opened;
		public final int closed;

		public ConnectionSnapshot(int inUse, int inPool, int opened, int closed) {
			this.inUse = inUse;
			this.inPool = inPool;
			this.opened = opened;
			this.closed = closed;
		}
	}

	/**
	 * Java event-loop measurements.
	 */
	public static final class EventLoopSnapshot {
		public final int processSize;
		public final int queueSize;

		public EventLoopSnapshot(int processSize, int queueSize) {
			this.processSize = processSize;
			this.queueSize = queueSize;
		}
	}

	/**
	 * Extended metrics for one namespace on one node.
	 */
	public static final class NamespaceSnapshot {
		public final String namespace;
		public final long errors;
		public final long timeouts;
		public final long keyBusy;
		public final long bytesIn;
		public final long bytesOut;

		/**
		 * Compatibility latency histograms for broad operation types
		 * (conn, read, write, batch, query).
		 */
		public final Map<LatencyType, HistogramSnapshot> compatibilityLatencies;

		/**
		 * Detailed sampled measurements by command type.
		 * Empty when no detailed measurements are available.
		 */
		public final Map<CommandType, CommandSnapshot> commands;

		public NamespaceSnapshot(
			String namespace,
			long errors,
			long timeouts,
			long keyBusy,
			long bytesIn,
			long bytesOut,
			Map<LatencyType, HistogramSnapshot> compatibilityLatencies,
			Map<CommandType, CommandSnapshot> commands
		) {
			this.namespace = namespace;
			this.errors = errors;
			this.timeouts = timeouts;
			this.keyBusy = keyBusy;
			this.bytesIn = bytesIn;
			this.bytesOut = bytesOut;
			this.compatibilityLatencies = Collections.unmodifiableMap(new HashMap<>(compatibilityLatencies));
			this.commands = Collections.unmodifiableMap(new HashMap<>(commands));
		}
	}

	/**
	 * Detailed sampled measurements for one command type within a namespace.
	 */
	public static final class CommandSnapshot {
		public final CommandType commandType;
		public final HistogramSnapshot connectionAcquisition;
		public final HistogramSnapshot requestWrite;
		public final HistogramSnapshot responseParse;
		public final HistogramSnapshot latency;
		public final HistogramSnapshot bytesSent;
		public final HistogramSnapshot bytesReceived;
		public final long retryCount;
		public final long errorCount;
		public final Map<Integer, Long> resultCodeCounts;

		public CommandSnapshot(
			CommandType commandType,
			HistogramSnapshot connectionAcquisition,
			HistogramSnapshot requestWrite,
			HistogramSnapshot responseParse,
			HistogramSnapshot latency,
			HistogramSnapshot bytesSent,
			HistogramSnapshot bytesReceived,
			long retryCount,
			long errorCount,
			Map<Integer, Long> resultCodeCounts
		) {
			this.commandType = commandType;
			this.connectionAcquisition = connectionAcquisition;
			this.requestWrite = requestWrite;
			this.responseParse = responseParse;
			this.latency = latency;
			this.bytesSent = bytesSent;
			this.bytesReceived = bytesReceived;
			this.retryCount = retryCount;
			this.errorCount = errorCount;
			this.resultCodeCounts = Collections.unmodifiableMap(new HashMap<>(resultCodeCounts));
		}
	}

	/**
	 * Histogram bucket counts and summary statistics. Latency histograms use
	 * {@link MetricsSnapshot#latencyUnit}; byte histograms use bytes.
	 */
	public static final class HistogramSnapshot {
		private final long[] buckets;
		public final long min;
		public final long max;
		public final double sum;
		public final long count;

		public HistogramSnapshot(long[] buckets, long min, long max, double sum, long count) {
			this.buckets = buckets.clone();
			this.min = min;
			this.max = max;
			this.sum = sum;
			this.count = count;
		}

		public long[] getBuckets() {
			return buckets.clone();
		}
	}

	// ── Enums ──────────────────────────────────────────────────────

	public enum HistogramType {
		LINEAR,
		LOGARITHMIC
	}

	public enum LatencyUnit {
		MILLISECONDS,
		MICROSECONDS
	}

	public enum CommandType {
		GET,
		GET_HEADER,
		EXISTS,
		PUT,
		DELETE,
		OPERATE,
		QUERY,
		SCAN,
		UDF,
		BATCH_READ,
		BATCH_WRITE
	}
}
