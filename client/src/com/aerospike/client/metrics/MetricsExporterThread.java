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
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ConnectionStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

/**
 * Daemon thread that periodically captures a {@link MetricsSnapshot} and
 * distributes it to all registered {@link IMetricsExporter} instances.
 * <p>
 * This thread is separate from the tend thread so that a slow exporter
 * never delays cluster tending.
 * <p>
 * Exporter failures are tracked independently. An exporter that exceeds
 * {@link MetricsPolicy#maxConsecutiveFailures} is suspended and retried
 * after {@link MetricsPolicy#suspendRetryInterval} seconds.
 */
public class MetricsExporterThread extends Thread {

	private final Cluster cluster;
	private final MetricsPolicy policy;
	private volatile boolean running = true;

	private final Map<IMetricsExporter, ExporterState> exporterStates = new HashMap<>();

	public MetricsExporterThread(Cluster cluster, MetricsPolicy policy) {
		super("aerospike-metrics-exporter");
		setDaemon(true);

		this.cluster = cluster;
		this.policy = policy;

		for (IMetricsExporter exporter : policy.getExporters()) {
			exporterStates.put(exporter, new ExporterState());
		}
	}

	@Override
	public void run() {
		Log.info("Metrics exporter thread started, interval=" + policy.interval + "s, exporters=" + policy.getExporters().size());

		while (running) {
			try {
				Thread.sleep(policy.interval * 1000L);
			}
			catch (InterruptedException e) {
				if (!running) {
					break;
				}
				Thread.currentThread().interrupt();
				break;
			}

			MetricsSnapshot snapshot;
			try {
				snapshot = buildSnapshot();
			}
			catch (Exception e) {
				Log.warn("Failed to capture metrics snapshot: " + Util.getErrorMessage(e));
				continue;
			}

			dispatch(snapshot);
		}

		Log.info("Metrics exporter thread stopped");
	}

	/**
	 * Signal the metrics exporter thread to stop and interrupt any sleep.
	 */
	public void shutdown() {
		running = false;
		interrupt();
	}

	// ── Snapshot builder ───────────────────────────────────────────

	private MetricsSnapshot buildSnapshot() {
		Node[] clusterNodes = cluster.getNodes();
		int totalNodeCount = clusterNodes.length;
		boolean extendedMetricsEnabled = cluster.metricsEnabled;

		long totalOpenConnections = 0;
		List<MetricsSnapshot.NodeSnapshot> nodeSnapshots = new ArrayList<>();

		for (Node node : clusterNodes) {
			MetricsSnapshot.NodeSnapshot nodeSnapshot = buildNodeSnapshot(node, extendedMetricsEnabled);
			nodeSnapshots.add(nodeSnapshot);
			totalOpenConnections += nodeSnapshot.openConnections;
		}

		MetricsSnapshot.NodeSnapshot aggregatedSnapshot = buildAggregatedSnapshot(nodeSnapshots);

		double cpuPercent = extendedMetricsEnabled ? Util.getProcessCpuLoad() : 0.0;
		long usedMemoryBytes = extendedMetricsEnabled
			? Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
			: 0L;
		long commandCount = extendedMetricsEnabled ? cluster.getCommandCount() : 0L;

		String clusterName = cluster.getClusterName();
		if (clusterName == null) {
			clusterName = "";
		}

		ClientPolicy clientPolicy = cluster.client.getClientPolicy();
		String applicationId = clientPolicy.appId != null ? clientPolicy.appId : "";

		List<MetricsSnapshot.EventLoopSnapshot> eventLoopSnapshots = buildEventLoopSnapshots();

		int latencyBase = 1 << policy.latencyShift;

		return new MetricsSnapshot(
			Instant.now(),
			extendedMetricsEnabled,
			clusterName,
			"java",
			cluster.client.getVersion(),
			applicationId,
			policy.labels,
			cluster.getRecoverQueueSize(),
			cluster.getInvalidNodeCount(),
			cluster.getRetryCount(),
			cluster.getDelayQueueTimeoutCount(),
			totalNodeCount,
			totalOpenConnections,
			0, // exceededMaxRetries — not yet tracked by client
			0, // exceededTotalTimeout — not yet tracked by client
			cpuPercent,
			usedMemoryBytes,
			commandCount,
			eventLoopSnapshots,
			nodeSnapshots,
			aggregatedSnapshot,
			MetricsSnapshot.HistogramType.LOGARITHMIC,
			MetricsSnapshot.LatencyUnit.MILLISECONDS,
			latencyBase,
			policy.latencyColumns
		);
	}

	private MetricsSnapshot.NodeSnapshot buildNodeSnapshot(Node node, boolean extendedMetricsEnabled) {
		String nodeName = node.getName();
		Host nodeHost = node.getHost();

		ConnectionStats syncConnectionStats = node.getConnectionStats();
		ConnectionStats asyncConnectionStats = node.getAsyncConnectionStats();

		MetricsSnapshot.ConnectionSnapshot syncSnapshot = new MetricsSnapshot.ConnectionSnapshot(
			syncConnectionStats.inUse, syncConnectionStats.inPool,
			syncConnectionStats.opened, syncConnectionStats.closed
		);
		MetricsSnapshot.ConnectionSnapshot asyncSnapshot = new MetricsSnapshot.ConnectionSnapshot(
			asyncConnectionStats.inUse, asyncConnectionStats.inPool,
			asyncConnectionStats.opened, asyncConnectionStats.closed
		);

		long openConnections = (long)(syncConnectionStats.inUse + syncConnectionStats.inPool
			+ asyncConnectionStats.inUse + asyncConnectionStats.inPool);

		List<MetricsSnapshot.NamespaceSnapshot> namespaceSnapshots = Collections.emptyList();
		Map<MetricsSnapshot.CommandType, MetricsSnapshot.HistogramSnapshot> commandLatencies = Collections.emptyMap();

		if (extendedMetricsEnabled) {
			namespaceSnapshots = buildNamespaceSnapshots(node);
		}

		return new MetricsSnapshot.NodeSnapshot(
			nodeName,
			nodeHost.name,
			nodeHost.port,
			syncSnapshot,
			asyncSnapshot,
			0, // connectionAttempts — not yet tracked
			0, // connectionsSuccessful — not yet tracked
			0, // connectionsFailed — not yet tracked
			0, // connectionTimeoutErrors — not yet tracked
			0, // connectionOtherErrors — not yet tracked
			0, // circuitBreakerHits — not yet tracked
			0, // connectionPoolEmptyCount — not yet tracked
			0, // connectionPoolOverflowCount — not yet tracked
			0, // idleConnectionsDropped — not yet tracked
			openConnections,
			0, // closedConnections — not yet tracked separately
			0, // recoveredConnections — not yet tracked
			0, // tendsTotal — not yet tracked per-node
			0, // tendsSuccessful — not yet tracked per-node
			0, // tendsFailed — not yet tracked per-node
			0, // partitionMapUpdates — not yet tracked per-node
			0, // nodesAdded — not yet tracked per-node
			0, // nodesRemoved — not yet tracked per-node
			0, // transactionRetryCount — not yet tracked per-node
			0, // transactionErrorCount — not yet tracked per-node
			commandLatencies,
			namespaceSnapshots
		);
	}

	private List<MetricsSnapshot.NamespaceSnapshot> buildNamespaceSnapshots(Node node) {
		NodeMetrics nodeMetrics = node.getMetrics();
		if (nodeMetrics == null) {
			return Collections.emptyList();
		}

		Histograms histograms = nodeMetrics.getHistograms();
		ConcurrentHashMap<String, LatencyBuckets[]> histogramsByNamespace = histograms.getMap();

		List<MetricsSnapshot.NamespaceSnapshot> namespaceSnapshots = new ArrayList<>();
		int latencyTypeCount = LatencyType.getMax();

		for (Map.Entry<String, LatencyBuckets[]> entry : histogramsByNamespace.entrySet()) {
			String namespace = entry.getKey();

			long errorCount = node.getErrorCountByNS(namespace);
			long timeoutCount = node.getTimeoutCountbyNS(namespace);
			long keyBusyCount = node.getKeyBusyCountByNS(namespace);
			long bytesInCount = node.getBytesInByNS(namespace);
			long bytesOutCount = node.getBytesOutByNS(namespace);

			LatencyBuckets[] latencyBucketsByType = entry.getValue();
			Map<LatencyType, MetricsSnapshot.HistogramSnapshot> compatibilityLatencies = new HashMap<>();

			for (int typeIndex = 0; typeIndex < latencyTypeCount; typeIndex++) {
				LatencyType latencyType = LatencyType.values()[typeIndex];
				LatencyBuckets buckets = latencyBucketsByType[typeIndex];
				int bucketCount = buckets.getMax();
				long[] bucketCounts = new long[bucketCount];
				long totalCount = 0;

				for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
					bucketCounts[bucketIndex] = buckets.getBucket(bucketIndex);
					totalCount += bucketCounts[bucketIndex];
				}

				compatibilityLatencies.put(latencyType, new MetricsSnapshot.HistogramSnapshot(
					bucketCounts, 0, 0, 0.0, totalCount
				));
			}

			Map<MetricsSnapshot.CommandType, MetricsSnapshot.CommandSnapshot> commandSnapshots =
				Collections.emptyMap();

			namespaceSnapshots.add(new MetricsSnapshot.NamespaceSnapshot(
				namespace, errorCount, timeoutCount, keyBusyCount, bytesInCount, bytesOutCount,
				compatibilityLatencies, commandSnapshots
			));
		}

		return namespaceSnapshots;
	}

	private MetricsSnapshot.NodeSnapshot buildAggregatedSnapshot(
			List<MetricsSnapshot.NodeSnapshot> nodeSnapshots) {
		int aggregatedSyncInUse = 0;
		int aggregatedSyncInPool = 0;
		int aggregatedSyncOpened = 0;
		int aggregatedSyncClosed = 0;
		int aggregatedAsyncInUse = 0;
		int aggregatedAsyncInPool = 0;
		int aggregatedAsyncOpened = 0;
		int aggregatedAsyncClosed = 0;
		long aggregatedOpenConnections = 0;

		for (MetricsSnapshot.NodeSnapshot nodeSnapshot : nodeSnapshots) {
			aggregatedSyncInUse += nodeSnapshot.syncConnections.inUse;
			aggregatedSyncInPool += nodeSnapshot.syncConnections.inPool;
			aggregatedSyncOpened += nodeSnapshot.syncConnections.opened;
			aggregatedSyncClosed += nodeSnapshot.syncConnections.closed;
			aggregatedAsyncInUse += nodeSnapshot.asyncConnections.inUse;
			aggregatedAsyncInPool += nodeSnapshot.asyncConnections.inPool;
			aggregatedAsyncOpened += nodeSnapshot.asyncConnections.opened;
			aggregatedAsyncClosed += nodeSnapshot.asyncConnections.closed;
			aggregatedOpenConnections += nodeSnapshot.openConnections;
		}

		MetricsSnapshot.ConnectionSnapshot aggregatedSyncConnections =
			new MetricsSnapshot.ConnectionSnapshot(
				aggregatedSyncInUse, aggregatedSyncInPool,
				aggregatedSyncOpened, aggregatedSyncClosed
			);
		MetricsSnapshot.ConnectionSnapshot aggregatedAsyncConnections =
			new MetricsSnapshot.ConnectionSnapshot(
				aggregatedAsyncInUse, aggregatedAsyncInPool,
				aggregatedAsyncOpened, aggregatedAsyncClosed
			);

		return new MetricsSnapshot.NodeSnapshot(
			"", "", 0,
			aggregatedSyncConnections,
			aggregatedAsyncConnections,
			0, // connectionAttempts
			0, // connectionsSuccessful
			0, // connectionsFailed
			0, // connectionTimeoutErrors
			0, // connectionOtherErrors
			0, // circuitBreakerHits
			0, // connectionPoolEmptyCount
			0, // connectionPoolOverflowCount
			0, // idleConnectionsDropped
			aggregatedOpenConnections,
			0, // closedConnections
			0, // recoveredConnections
			0, // tendsTotal
			0, // tendsSuccessful
			0, // tendsFailed
			0, // partitionMapUpdates
			0, // nodesAdded
			0, // nodesRemoved
			0, // transactionRetryCount
			0, // transactionErrorCount
			Collections.emptyMap(),
			Collections.emptyList()
		);
	}

	private List<MetricsSnapshot.EventLoopSnapshot> buildEventLoopSnapshots() {
		EventLoop[] eventLoops = cluster.getEventLoopArray();
		if (eventLoops == null) {
			return Collections.emptyList();
		}

		List<MetricsSnapshot.EventLoopSnapshot> eventLoopSnapshots = new ArrayList<>(eventLoops.length);
		for (EventLoop eventLoop : eventLoops) {
			eventLoopSnapshots.add(new MetricsSnapshot.EventLoopSnapshot(
				eventLoop.getProcessSize(), eventLoop.getQueueSize()
			));
		}
		return eventLoopSnapshots;
	}

	// ── Exporter dispatch ──────────────────────────────────────────

	private void dispatch(MetricsSnapshot snapshot) {
		for (IMetricsExporter exporter : policy.getExporters()) {
			ExporterState state = exporterStates.get(exporter);
			if (state == null) {
				state = new ExporterState();
				exporterStates.put(exporter, state);
			}

			if (state.suspended) {
				long elapsed = System.currentTimeMillis() - state.suspendedAt;
				if (elapsed < policy.suspendRetryInterval * 1000L) {
					continue;
				}
				Log.info("Retrying suspended exporter: " + exporter.getClass().getSimpleName());
			}

			try {
				exporter.export(snapshot);

				if (state.suspended) {
					Log.info("Exporter " + exporter.getClass().getSimpleName() + " resumed after successful retry");
				}
				state.reset();
			}
			catch (Exception e) {
				state.consecutiveFailures++;
				Log.warn("Exporter " + exporter.getClass().getSimpleName()
					+ " failed (consecutive=" + state.consecutiveFailures
					+ "): " + Util.getErrorMessage(e));

				if (state.consecutiveFailures >= policy.maxConsecutiveFailures) {
					state.suspended = true;
					state.suspendedAt = System.currentTimeMillis();
					Log.error("Exporter " + exporter.getClass().getSimpleName()
						+ " suspended after " + state.consecutiveFailures + " consecutive failures");
				}
			}
		}
	}

	// ── Exporter state tracking ────────────────────────────────────

	private static final class ExporterState {
		int consecutiveFailures;
		boolean suspended;
		long suspendedAt;

		void reset() {
			consecutiveFailures = 0;
			suspended = false;
			suspendedAt = 0;
		}
	}
}
