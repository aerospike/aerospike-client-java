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
package com.aerospike.examples;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.LatencySnapshot;
import com.aerospike.client.metrics.MetricsExporterType;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;
import com.aerospike.client.metrics.NamespaceMetricsSnapshot;
import com.aerospike.client.metrics.NodeMetricsSnapshot;

/**
 * Demonstrate custom and file metrics exporters with standard, operational,
 * and usage configuration.
 */
public class Metrics extends Example {
	private static final Duration ExportInterval = Duration.ofSeconds(2);
	private static final long OperationDurationMillis = 3_000;

	@Override
	public void runExample() throws Exception {
		IAerospikeClient client = client();
		Parameters params = params();

		runCustomExporterExample(client, params);
		runFileExporterExample(client, params);
	}

	/**
	 * Export structured standard, operational, and usage snapshots to two
	 * application-owned exporters in registration order.
	 */
	private void runCustomExporterExample(
		IAerospikeClient client,
		Parameters params
	) throws Exception {
		SampleMetricsExporter firstExporter =
			new SampleMetricsExporter("custom-1");
		SampleMetricsExporter secondExporter =
			new SampleMetricsExporter("custom-2");
		MetricsPolicy policy = new MetricsPolicy();
		policy.exportInterval = ExportInterval;
		policy.exporter = MetricsExporterType.CUSTOM;
		policy.enableOperationalMetrics = true;
		policy.enableUsageMetrics = true;
		policy.addExporter(firstExporter);
		policy.addExporter(secondExporter);

		console.info("Use case 1: two CUSTOM exporters");
		try {
			client.enableMetrics(policy);
			runOperations(client, params, OperationDurationMillis);
		}
		finally {
			// disableMetrics() waits for one final metrics-thread export. The
			// application owns and closes resources used by a custom exporter.
			client.disableMetrics();
		}

		verifyCustomExporters(firstExporter, secondExporter);
		console.info("Both custom exporters received %d snapshots",
			firstExporter.snapshotCount.get());
		console.info("Usage delivery is enabled; usage counters remain zero until " +
			"their collection providers are implemented.");
	}

	/**
	 * Export standard and operational metrics to the client-owned legacy metrics
	 * file and verify that the flushed output can be read after disable.
	 */
	private void runFileExporterExample(
		IAerospikeClient client,
		Parameters params
	) throws Exception {
		Path reportDirectory =
			Files.createTempDirectory("aerospike-metrics-example-");
		MetricsPolicy policy = new MetricsPolicy();
		policy.exportInterval = ExportInterval;
		policy.exporter = MetricsExporterType.FILE;
		policy.reportDir = reportDirectory.toString();
		policy.enableOperationalMetrics = true;

		// Usage is intentionally disabled for FILE. The current legacy positional
		// format carries standard and operational fields, while the tech spec
		// defers usage fields until the next breaking file-format revision.
		policy.enableUsageMetrics = false;

		console.info("Use case 2: FILE exporter");
		try {
			client.enableMetrics(policy);
			runOperations(client, params, OperationDurationMillis);
		}
		finally {
			// The final snapshot is written and flushed before disable returns.
			client.disableMetrics();
		}

		Path output = findMetricsFile(reportDirectory);
		List<String> lines = Files.readAllLines(output);

		if (lines.size() < 2 || !lines.get(0).contains("header(2)") ||
			!lines.get(1).contains(" cluster[")) {
			throw new IllegalStateException(
				"FILE exporter did not produce a valid metrics log");
		}
		console.info("Metrics file contains %d lines: %s", lines.size(), output);
	}

	/**
	 * Verify that both custom exporters received periodic and final snapshots
	 * with the requested metric groups.
	 */
	private void verifyCustomExporters(
		SampleMetricsExporter first,
		SampleMetricsExporter second
	) {
		if (first.snapshotCount.get() != second.snapshotCount.get() ||
			first.snapshotCount.get() < 2) {
			throw new IllegalStateException(
				"Custom exporters did not receive the same periodic and final snapshots");
		}
		if (!first.periodicSnapshotReceived || !first.finalSnapshotReceived ||
			!second.periodicSnapshotReceived || !second.finalSnapshotReceived) {
			throw new IllegalStateException(
				"Custom exporters did not receive periodic and final snapshots");
		}
		if (!first.operationalMetricsReceived || !first.usageMetricsReceived ||
			!second.operationalMetricsReceived || !second.usageMetricsReceived) {
			throw new IllegalStateException(
				"Custom snapshots did not enable operational and usage metrics");
		}
	}

	/**
	 * Return the metrics file created in the example report directory.
	 */
	private Path findMetricsFile(Path reportDirectory) throws Exception {
		try (Stream<Path> files = Files.list(reportDirectory)) {
			return files
				.filter(path -> path.getFileName().toString().startsWith("metrics-"))
				.findFirst()
				.orElseThrow(() -> new IllegalStateException(
					"FILE exporter did not create a metrics log"));
		}
	}

	/**
	 * Perform put/get operations for the specified duration in milliseconds.
	 */
	private void runOperations(IAerospikeClient client, Parameters params, long durationMs) throws Exception {
		long endTime = System.currentTimeMillis() + durationMs;
		int operationCount = 0;

		while (System.currentTimeMillis() < endTime) {
			Key key = new Key(params.namespace, params.set, "metrics-test-" + operationCount);
			Bin bin = new Bin("val", operationCount);
			client.put(params.writePolicy, key, bin);

			Record record = client.get(params.policy, key);

			if (record == null) {
				console.error("Failed to get key: metrics-test-" + operationCount);
			}
			operationCount++;
			Thread.sleep(100);
		}
		console.info("Performed %d put/get operations", operationCount);
	}

	/**
	 * Sample custom exporter that prints each structured snapshot and records
	 * which lifecycle and metric-group states it received.
	 */
	private class SampleMetricsExporter implements IMetricsExporter {
		final String label;
		final AtomicInteger snapshotCount = new AtomicInteger();
		volatile boolean periodicSnapshotReceived;
		volatile boolean finalSnapshotReceived;
		volatile boolean operationalMetricsReceived;
		volatile boolean usageMetricsReceived;

		SampleMetricsExporter(String label) {
			this.label = label;
		}

		@Override
		public void export(MetricsSnapshot snapshot) {
			int currentSnapshot = snapshotCount.incrementAndGet();
			int namespaceCount = 0;
			periodicSnapshotReceived |= snapshot.metricsEnabled;
			finalSnapshotReceived |= !snapshot.metricsEnabled;
			operationalMetricsReceived |= snapshot.operationalMetricsEnabled;
			usageMetricsReceived |= snapshot.usageMetricsEnabled;

			console.info("[%s] --- Metrics Snapshot #%d ---", label, currentSnapshot);
			console.info("  Timestamp:      %s", snapshot.timestamp);
			console.info("  Metrics enabled:     %s", snapshot.metricsEnabled);
			console.info("  Operational enabled: %s",
				snapshot.operationalMetricsEnabled);
			console.info("  Usage enabled:       %s", snapshot.usageMetricsEnabled);
			console.info("  Cluster:        %s", snapshot.clusterName);
			console.info("  Nodes:          %d", snapshot.totalNodes);
			console.info("  Open conns:     %d", snapshot.openConnections);
			console.info("  CPU:            %.2f%%", snapshot.cpuPercent);
			console.info("  Memory (bytes): %d", snapshot.memoryBytes);
			console.info("  Command count:  %d", snapshot.commandCount);
			console.info("  Command retries:%d", snapshot.commandRetries);
			console.info("  Read latency samples:  %d",
				latencyCount(snapshot.readLatency));
			console.info("  Write latency samples: %d",
				latencyCount(snapshot.writeLatency));
			console.info("  Usage: %s", snapshot.usage);

			for (NodeMetricsSnapshot nodeSnapshot : snapshot.nodes) {
				console.info("  Node %s (%s:%d):", nodeSnapshot.nodeName,
					nodeSnapshot.nodeAddress, nodeSnapshot.nodePort);
				console.info("    Sync conns:  inUse=%d inPool=%d opened=%d closed=%d",
					nodeSnapshot.syncConnections.inUse, nodeSnapshot.syncConnections.inPool,
					nodeSnapshot.syncConnections.opened, nodeSnapshot.syncConnections.closed);
				console.info("    Async conns: inUse=%d inPool=%d opened=%d closed=%d",
					nodeSnapshot.asyncConnections.inUse, nodeSnapshot.asyncConnections.inPool,
					nodeSnapshot.asyncConnections.opened, nodeSnapshot.asyncConnections.closed);
				console.info("    Open conns:  %d", nodeSnapshot.openConnections);

				namespaceCount += nodeSnapshot.namespaces.size();

				for (NamespaceMetricsSnapshot namespaceSnapshot : nodeSnapshot.namespaces) {
					console.info("    Namespace %s: errors=%d timeouts=%d keyBusy=%d bytesIn=%d bytesOut=%d",
						namespaceSnapshot.namespace, namespaceSnapshot.errors,
						namespaceSnapshot.clientTimeouts, namespaceSnapshot.keyBusy,
						namespaceSnapshot.bytesIn, namespaceSnapshot.bytesOut);
				}
			}
			console.info("  Namespaces:     %d", namespaceCount);
		}

		private long latencyCount(LatencySnapshot latency) {
			return latency == null ? 0 : latency.count;
		}

	}
}
