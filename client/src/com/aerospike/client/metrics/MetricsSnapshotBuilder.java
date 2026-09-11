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
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ConnectionStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

/**
 * Builds immutable point-in-time metrics snapshots from client state.
 */
final class MetricsSnapshotBuilder {
    private final Cluster cluster;
    private final MetricsPolicy policy;

    MetricsSnapshotBuilder(Cluster cluster, MetricsPolicy policy) {
        this.cluster = cluster;
        this.policy = policy;
    }

    MetricsSnapshot build() {
        Node[] clusterNodes = cluster.getNodes();
        boolean extendedMetricsEnabled = policy.enableExtendedMetrics;
        long totalOpenConnections = 0;
        List<MetricsSnapshot.NodeSnapshot> nodeSnapshots = new ArrayList<>(clusterNodes.length);

        for (Node node : clusterNodes) {
            MetricsSnapshot.NodeSnapshot nodeSnapshot =
                    buildNodeSnapshot(node, extendedMetricsEnabled);
            nodeSnapshots.add(nodeSnapshot);
            totalOpenConnections += nodeSnapshot.openConnections;
        }

        MetricsSnapshot.NodeSnapshot aggregatedSnapshot =
                buildAggregatedSnapshot(nodeSnapshots);
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
                clusterNodes.length,
                totalOpenConnections,
                0, // exceededMaxRetries — not yet tracked by client
                0, // exceededTotalTimeout — not yet tracked by client
                cpuPercent,
                usedMemoryBytes,
                commandCount,
                buildEventLoopSnapshots(),
                nodeSnapshots,
                aggregatedSnapshot,
                MetricsSnapshot.HistogramType.LOGARITHMIC,
                MetricsSnapshot.LatencyUnit.MILLISECONDS,
                latencyBase,
                policy.latencyColumns
        );
    }

    private MetricsSnapshot.NodeSnapshot buildNodeSnapshot(
            Node node,
            boolean extendedMetricsEnabled
    ) {
        Host nodeHost = node.getHost();
        ConnectionStats syncStats = node.getConnectionStats();
        ConnectionStats asyncStats = node.getAsyncConnectionStats();
        MetricsSnapshot.ConnectionSnapshot syncSnapshot = buildConnectionSnapshot(syncStats);
        MetricsSnapshot.ConnectionSnapshot asyncSnapshot = buildConnectionSnapshot(asyncStats);
        long openConnections = (long) (syncStats.inUse + syncStats.inPool
                + asyncStats.inUse + asyncStats.inPool);
        List<MetricsSnapshot.NamespaceSnapshot> namespaces = extendedMetricsEnabled
                ? buildNamespaceSnapshots(node)
                : Collections.emptyList();

        return new MetricsSnapshot.NodeSnapshot(
                node.getName(),
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
                Collections.emptyMap(),
                namespaces
        );
    }

    private MetricsSnapshot.ConnectionSnapshot buildConnectionSnapshot(ConnectionStats stats) {
        return new MetricsSnapshot.ConnectionSnapshot(
                stats.inUse, stats.inPool, stats.opened, stats.closed
        );
    }

    private List<MetricsSnapshot.NamespaceSnapshot> buildNamespaceSnapshots(Node node) {
        NodeMetrics nodeMetrics = node.getMetrics();

        if (nodeMetrics == null) {
            return Collections.emptyList();
        }

        Histograms histograms = nodeMetrics.getHistograms();
        ConcurrentHashMap<String, LatencyBuckets[]> histogramsByNamespace =
                histograms.getMap();
        List<MetricsSnapshot.NamespaceSnapshot> namespaceSnapshots =
                new ArrayList<>(histogramsByNamespace.size());
        int latencyTypeCount = LatencyType.getMax();

        for (Map.Entry<String, LatencyBuckets[]> entry : histogramsByNamespace.entrySet()) {
            String namespace = entry.getKey();
            LatencyBuckets[] latencyBucketsByType = entry.getValue();
            Map<LatencyType, MetricsSnapshot.HistogramSnapshot> compatibilityLatencies =
                    new HashMap<>(latencyTypeCount);

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

                compatibilityLatencies.put(latencyType,
                        new MetricsSnapshot.HistogramSnapshot(
                                bucketCounts, 0, 0, 0.0, totalCount
                        )
                );
            }

            namespaceSnapshots.add(new MetricsSnapshot.NamespaceSnapshot(
                    namespace,
                    node.getErrorCountByNS(namespace),
                    node.getTimeoutCountbyNS(namespace),
                    node.getKeyBusyCountByNS(namespace),
                    node.getBytesInByNS(namespace),
                    node.getBytesOutByNS(namespace),
                    compatibilityLatencies,
                    Collections.emptyMap()
            ));
        }
        return namespaceSnapshots;
    }

    private MetricsSnapshot.NodeSnapshot buildAggregatedSnapshot(
            List<MetricsSnapshot.NodeSnapshot> nodeSnapshots
    ) {
        int syncInUse = 0;
        int syncInPool = 0;
        int syncOpened = 0;
        int syncClosed = 0;
        int asyncInUse = 0;
        int asyncInPool = 0;
        int asyncOpened = 0;
        int asyncClosed = 0;
        long openConnections = 0;

        for (MetricsSnapshot.NodeSnapshot node : nodeSnapshots) {
            syncInUse += node.syncConnections.inUse;
            syncInPool += node.syncConnections.inPool;
            syncOpened += node.syncConnections.opened;
            syncClosed += node.syncConnections.closed;
            asyncInUse += node.asyncConnections.inUse;
            asyncInPool += node.asyncConnections.inPool;
            asyncOpened += node.asyncConnections.opened;
            asyncClosed += node.asyncConnections.closed;
            openConnections += node.openConnections;
        }

        return new MetricsSnapshot.NodeSnapshot(
                "",
                "",
                0,
                new MetricsSnapshot.ConnectionSnapshot(
                        syncInUse, syncInPool, syncOpened, syncClosed
                ),
                new MetricsSnapshot.ConnectionSnapshot(
                        asyncInUse, asyncInPool, asyncOpened, asyncClosed
                ),
                0, // connectionAttempts
                0, // connectionsSuccessful
                0, // connectionsFailed
                0, // connectionTimeoutErrors
                0, // connectionOtherErrors
                0, // circuitBreakerHits
                0, // connectionPoolEmptyCount
                0, // connectionPoolOverflowCount
                0, // idleConnectionsDropped
                openConnections,
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

        List<MetricsSnapshot.EventLoopSnapshot> snapshots =
                new ArrayList<>(eventLoops.length);

        for (EventLoop eventLoop : eventLoops) {
            snapshots.add(new MetricsSnapshot.EventLoopSnapshot(
                    eventLoop.getProcessSize(), eventLoop.getQueueSize()
            ));
        }
        return snapshots;
    }
}
