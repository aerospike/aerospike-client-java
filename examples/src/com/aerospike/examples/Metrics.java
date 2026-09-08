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

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;

/**
 * Demonstrate IMetricsExporter integration. Registers a sample exporter that
 * prints snapshot summaries, enables metrics, performs some operations, and
 * shows the snapshot data that the exporter receives.
 */
public class Metrics extends Example {

	public Metrics(Console console) {
		super(console);
	}

	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		SampleMetricsExporter extendedExporter =
			runMetricsPhase(client, params, "extended-on", true);
		SampleMetricsExporter standardExporter =
			runMetricsPhase(client, params, "extended-off", false);

		console.info("");
		console.info("=== Comparison ===");
		console.info("Extended ON  — snapshots: %d, last CPU: %.2f%%, last namespaces: %d",
			extendedExporter.snapshotCount.get(), extendedExporter.lastCpuPercent,
			extendedExporter.lastNamespaceCount);
		console.info("Extended OFF — snapshots: %d, last CPU: %.2f%%, last namespaces: %d",
			standardExporter.snapshotCount.get(), standardExporter.lastCpuPercent,
			standardExporter.lastNamespaceCount);
	}

	private SampleMetricsExporter runMetricsPhase(
		IAerospikeClient client,
		Parameters params,
		String label,
		boolean enableExtendedMetrics
	) throws Exception {
		console.info("");
		console.info("=== Extended metrics %s ===",
			enableExtendedMetrics ? "ENABLED" : "DISABLED");

		SampleMetricsExporter exporter = new SampleMetricsExporter(label);
		MetricsPolicy policy = new MetricsPolicy();
		policy.interval = 5;
		policy.enableExtendedMetrics = enableExtendedMetrics;
		policy.addExporter(exporter);

		// The legacy file listener remains enabled for backwards compatibility.
		// Set policy.reportDir to choose where those log files are written.
		try {
			client.enableMetrics(policy);
			runOperations(client, params, 12_000);
			console.info("Phase complete: %d snapshots received",
				exporter.snapshotCount.get());
			return exporter;
		}
		finally {
			client.disableMetrics();
			exporter.close();
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
	 * Sample IMetricsExporter that prints snapshot summaries to the console.
	 * Tracks the last CPU percentage and namespace count for comparison.
	 */
	private class SampleMetricsExporter implements IMetricsExporter {
		final String label;
		final AtomicInteger snapshotCount = new AtomicInteger();
		volatile double lastCpuPercent;
		volatile int lastNamespaceCount;

		SampleMetricsExporter(String label) {
			this.label = label;
		}

		@Override
		public void export(MetricsSnapshot snapshot) {
			int currentSnapshot = snapshotCount.incrementAndGet();
			lastCpuPercent = snapshot.cpuPercent;
			int namespaceCount = 0;

			console.info("[%s] --- Metrics Snapshot #%d ---", label, currentSnapshot);
			console.info("  Timestamp:      %s", snapshot.timestamp);
			console.info("  Extended:       %s", snapshot.extendedMetricsEnabled);
			console.info("  Cluster:        %s", snapshot.clusterName);
			console.info("  Nodes:          %d", snapshot.totalNodes);
			console.info("  Open conns:     %d", snapshot.openConnections);
			console.info("  CPU:            %.2f%%", snapshot.cpuPercent);
			console.info("  Memory (bytes): %d", snapshot.memoryBytes);
			console.info("  Retry count:    %d", snapshot.retryCount);
			console.info("  Command count:  %d", snapshot.commandCount);

			for (MetricsSnapshot.NodeSnapshot nodeSnapshot : snapshot.nodes) {
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

				for (MetricsSnapshot.NamespaceSnapshot namespaceSnapshot : nodeSnapshot.namespaces) {
					console.info("    Namespace %s: errors=%d timeouts=%d keyBusy=%d bytesIn=%d bytesOut=%d",
						namespaceSnapshot.namespace, namespaceSnapshot.errors,
						namespaceSnapshot.timeouts, namespaceSnapshot.keyBusy,
						namespaceSnapshot.bytesIn, namespaceSnapshot.bytesOut);
				}
			}
			lastNamespaceCount = namespaceCount;
		}

		@Override
		public void close() {
			console.info("[%s] Sample exporter closed", label);
		}
	}
}
