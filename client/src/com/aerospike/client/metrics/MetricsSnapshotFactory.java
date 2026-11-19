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

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ConnectionStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

/**
 * Creates immutable point-in-time metrics snapshots from client state.
 */
final class MetricsSnapshotFactory {
	private final Cluster cluster;
	private final MetricsPolicy policy;

	/**
	 * Initialize a factory for one client cluster and metrics policy.
	 */
	MetricsSnapshotFactory(Cluster cluster, MetricsPolicy policy) {
		this.cluster = cluster;
		this.policy = policy;
	}

	/**
	 * Capture current client state in one immutable snapshot.
	 *
	 * @param metricsEnabled whether metrics are enabled for this snapshot
	 */
	MetricsSnapshot create(boolean metricsEnabled) {
		Node[] clusterNodes = cluster.getNodes();
		boolean operationalMetricsEnabled = policy.enableOperationalMetrics;
		long totalOpenConnections = 0;
		List<NodeMetricsSnapshot> nodeSnapshots = new ArrayList<>(clusterNodes.length);

		for (Node node : clusterNodes) {
			NodeMetricsSnapshot nodeSnapshot =
				createNodeSnapshot(node, operationalMetricsEnabled);
			nodeSnapshots.add(nodeSnapshot);
			totalOpenConnections += nodeSnapshot.openConnections;
		}

		double cpuPercent = operationalMetricsEnabled ? Util.getProcessCpuLoad() : 0.0;
		long usedMemoryBytes = operationalMetricsEnabled
			? Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
			: 0L;
		long commandCount = operationalMetricsEnabled ? cluster.getCommandCount() : 0L;
		String clusterName = cluster.getClusterName();

		if (clusterName == null) {
			clusterName = "";
		}

		ClientPolicy clientPolicy = cluster.client.getClientPolicy();
		String applicationId = resolveApplicationId(clientPolicy);

		return new MetricsSnapshot(
			Instant.now(),
			metricsEnabled,
			operationalMetricsEnabled,
			policy.enableUsageMetrics,
			clusterName,
			"java",
			cluster.client.getVersion(),
			applicationId,
			policy.labels,
			clusterNodes.length,
			cluster.getRecoverQueueSize(),
			cluster.getInvalidNodeCount(),
			cluster.getDelayQueueTimeoutCount(),
			totalOpenConnections,
			commandCount,
			cluster.getRetryCount(),
			0, // TODO CM-28: map batch partial failures when collection is implemented.
			0, // TODO CM-28: collect the approved Java exceededMaxRetries extension.
			0, // TODO CM-28: collect the approved Java exceededTotalTimeout extension.
			cpuPercent,
			usedMemoryBytes,
			aggregateLatency(nodeSnapshots, LatencyType.READ),
			aggregateLatency(nodeSnapshots, LatencyType.WRITE),
			aggregateLatency(nodeSnapshots, LatencyType.BATCH),
			aggregateLatency(nodeSnapshots, LatencyType.QUERY),
			0, // TODO CM-28: map connection validation open failures.
			0, // TODO CM-28: map connection validation TLS failures.
			0, // TODO CM-28: map connection validation authentication failures.
			createEventLoopSnapshots(),
			nodeSnapshots,
			// TODO CM-28: use the collection-owned departed-node provider when available.
			// TODO: Confirm departed-node drain semantics with the Clients team.
			// Current intended behavior matches the Java and Python SDKs: departed
			// nodes are marked reported once included, regardless of exporter success.
			Collections.emptyList(),
			createUsageSnapshot(),
			policy.latencyUnit,
			policy.latencyColumns,
			policy.latencyShift
		);
	}

	private String resolveApplicationId(ClientPolicy clientPolicy) {
		if (clientPolicy.appId != null && !clientPolicy.appId.isEmpty()) {
			return clientPolicy.appId;
		}

		byte[] user = cluster.getUser();
		if (user != null && user.length > 0) {
			return new String(user, StandardCharsets.UTF_8);
		}
		return "not-set";
	}

	private UsageMetricsSnapshot createUsageSnapshot() {
		// TODO CM-28: populate approved usage metrics when collection providers are available.
		return new UsageMetricsSnapshot(
			0, // blocking
			0, // deferred
			0, // background
			0, // shapePoint
			0, // shapeBatch
			0, // shapeQuery
			0, // objectMappingRead
			0, // objectMappingWrite
			0, // filterAel
			0  // filterExp
		);
	}

	private NodeMetricsSnapshot createNodeSnapshot(
		Node node,
		boolean extendedMetricsEnabled
	) {
		Host nodeHost = node.getHost();
		ConnectionStats syncStats = node.getConnectionStats();
		ConnectionStats asyncStats = node.getAsyncConnectionStats();
		ConnectionMetricsSnapshot syncSnapshot = createConnectionSnapshot(syncStats);
		ConnectionMetricsSnapshot asyncSnapshot = createConnectionSnapshot(asyncStats);
		long openConnections = (long)(syncStats.inUse + syncStats.inPool
			+ asyncStats.inUse + asyncStats.inPool);
		List<NamespaceMetricsSnapshot> namespaces = extendedMetricsEnabled
			? createNamespaceSnapshots(node)
			: Collections.emptyList();

		return new NodeMetricsSnapshot(
			node.getName(),
			nodeHost.name,
			nodeHost.port,
			syncSnapshot,
			asyncSnapshot,
			openConnections,
			extendedMetricsEnabled ? createNodeConnectionLatency(node) : null,
			0, // TODO CM-28: map connection open failures when collection is implemented.
			0, // TODO CM-28: map TLS handshake failures when collection is implemented.
			0, // TODO CM-28: map authentication failures when collection is implemented.
			namespaces
		);
	}

	private ConnectionMetricsSnapshot createConnectionSnapshot(ConnectionStats stats) {
		return new ConnectionMetricsSnapshot(
			stats.inUse, stats.inPool, stats.opened, stats.closed
		);
	}

	private List<NamespaceMetricsSnapshot> createNamespaceSnapshots(Node node) {
		NodeMetrics nodeMetrics = node.getMetrics();

		if (nodeMetrics == null) {
			return Collections.emptyList();
		}

		Set<String> namespaces = node.getMetricNamespaces();
		List<NamespaceMetricsSnapshot> namespaceSnapshots =
			new ArrayList<>(namespaces.size());

		for (String namespace : namespaces) {
			namespaceSnapshots.add(new NamespaceMetricsSnapshot(
				namespace,
				node.getErrorCountByNS(namespace),
				node.getTimeoutCountbyNS(namespace),
				0, // TODO CM-28: distinguish server timeouts from client timeouts.
				node.getKeyBusyCountByNS(namespace),
				0, // TODO CM-28: collect record-too-big results by namespace.
				0, // TODO CM-28: collect device-overload results by namespace.
				node.getBytesInByNS(namespace),
				node.getBytesOutByNS(namespace),
				createLatency(node, namespace, LatencyType.READ),
				createLatency(node, namespace, LatencyType.WRITE),
				createLatency(node, namespace, LatencyType.BATCH),
				createLatency(node, namespace, LatencyType.QUERY)
			));
		}
		return namespaceSnapshots;
	}

	private LatencySnapshot createNodeConnectionLatency(Node node) {
		return aggregateNodeLatency(node, LatencyType.CONN);
	}

	private LatencySnapshot aggregateNodeLatency(Node node, LatencyType type) {
		long[] totals = null;
		long count = 0;

		for (String namespace : node.getMetricNamespaces()) {
			LatencySnapshot latency = createLatency(node, namespace, type);
			if (latency == null) {
				continue;
			}

			long[] buckets = latency.getBucketCounts();
			if (totals == null) {
				totals = new long[buckets.length];
			}
			for (int i = 0; i < buckets.length; i++) {
				totals[i] += buckets[i];
			}
			count += latency.count;
		}
		return totals == null ? null :
			new LatencySnapshot(type, totals, null, null, null, count);
	}

	private LatencySnapshot createLatency(Node node, String namespace, LatencyType type) {
		LatencyBuckets[] bucketsByType = node.getMetrics().getHistograms().getMap().get(namespace);
		if (bucketsByType == null) {
			return null;
		}

		LatencyBuckets buckets = bucketsByType[type.ordinal()];
		long[] bucketCounts = new long[buckets.getMax()];
		long count = 0;

		for (int i = 0; i < bucketCounts.length; i++) {
			bucketCounts[i] = buckets.getBucket(i);
			count += bucketCounts[i];
		}

		// TODO CM-28: map min/max/sum when collection exposes measured summaries.
		return new LatencySnapshot(type, bucketCounts, null, null, null, count);
	}

	private LatencySnapshot aggregateLatency(
		List<NodeMetricsSnapshot> nodeSnapshots,
		LatencyType type
	) {
		long[] totals = null;
		long count = 0;

		for (NodeMetricsSnapshot node : nodeSnapshots) {
			for (NamespaceMetricsSnapshot namespace : node.namespaces) {
				LatencySnapshot latency = latencyFor(namespace, type);
				if (latency == null) {
					continue;
				}
				long[] buckets = latency.getBucketCounts();
				if (totals == null) {
					totals = new long[buckets.length];
				}
				for (int i = 0; i < buckets.length; i++) {
					totals[i] += buckets[i];
				}
				count += latency.count;
			}
		}
		return totals == null ? null :
			new LatencySnapshot(type, totals, null, null, null, count);
	}

	private LatencySnapshot latencyFor(NamespaceMetricsSnapshot namespace, LatencyType type) {
		return switch (type) {
		case READ -> namespace.readLatency;
		case WRITE -> namespace.writeLatency;
		case BATCH -> namespace.batchLatency;
		case QUERY -> namespace.queryLatency;
		default -> null;
		};
	}

	private List<EventLoopMetricsSnapshot> createEventLoopSnapshots() {
		EventLoop[] eventLoops = cluster.getEventLoopArray();

		if (eventLoops == null) {
			return Collections.emptyList();
		}

		List<EventLoopMetricsSnapshot> snapshots =
			new ArrayList<>(eventLoops.length);

		for (EventLoop eventLoop : eventLoops) {
			snapshots.add(new EventLoopMetricsSnapshot(
				eventLoop.getProcessSize(), eventLoop.getQueueSize()
			));
		}
		return snapshots;
	}
}
