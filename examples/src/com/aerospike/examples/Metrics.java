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

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;

/**
 * Demonstrate IMetricsExporter integration. Registers a simple console-printing
 * exporter, enables metrics, performs some operations, and shows the snapshot
 * data that the exporter receives.
 */
public class Metrics extends Example {

	public Metrics(Console console) {
		super(console);
	}

	@Override
	public void runExample(IAerospikeClient client, Parameters params) throws Exception {
		ConsoleMetricsExporter exporter = new ConsoleMetricsExporter();

		MetricsPolicy metricsPolicy = new MetricsPolicy();
		metricsPolicy.interval = 5;
		metricsPolicy.addExporter(exporter);

		console.info("Enabling metrics with interval=5s and console exporter");
		client.enableMetrics(metricsPolicy);

		console.info("Performing read/write operations for ~12 seconds...");
		long endTime = System.currentTimeMillis() + 12_000;
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
		console.info("Exporter received %d snapshots", exporter.snapshotCount);
		console.info("Disabling metrics");
		client.disableMetrics();
	}

	/**
	 * Simple IMetricsExporter that prints snapshot summaries to the console.
	 */
	private class ConsoleMetricsExporter implements IMetricsExporter {
		int snapshotCount = 0;

		@Override
		public void export(MetricsSnapshot snapshot) {
			snapshotCount++;
			console.info("--- Metrics Snapshot #%d ---", snapshotCount);
			console.info("  Timestamp:      %s", snapshot.timestamp);
			console.info("  Cluster:        %s", snapshot.clusterName);
			console.info("  Nodes:          %d", snapshot.totalNodes);
			console.info("  Open conns:     %d", snapshot.openConnections);
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

				for (MetricsSnapshot.NamespaceSnapshot namespaceSnapshot : nodeSnapshot.namespaces) {
					console.info("    Namespace %s: errors=%d timeouts=%d keyBusy=%d bytesIn=%d bytesOut=%d",
						namespaceSnapshot.namespace, namespaceSnapshot.errors,
						namespaceSnapshot.timeouts, namespaceSnapshot.keyBusy,
						namespaceSnapshot.bytesIn, namespaceSnapshot.bytesOut);
				}
			}
		}

		@Override
		public void close() {
			console.info("Console exporter closed");
		}
	}
}
