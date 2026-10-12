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
package com.aerospike.test.sync.basic;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.sync.TestSync;

/**
 * Integration tests for the metrics exporter framework.
 * These tests require a running Aerospike server.
 */
public class TestMetricsExporter extends TestSync {

	private static final long SNAPSHOT_TIMEOUT_MILLIS = 5000;

	@Test
	public void testExporterReceivesValidSnapshot() throws Exception {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.addExporter(exporter);

		MetricsSnapshot snapshot;
		try {
			client.enableMetrics(policy);
			snapshot = waitForSnapshot(exporter, value -> true);
		}
		finally {
			client.disableMetrics();
		}

		assertNotNull("Exporter should receive a snapshot", snapshot);
		assertTrue("Extended metrics should be enabled by default",
			snapshot.extendedMetricsEnabled);
		assertTrue("Should have at least 1 node", snapshot.totalNodes >= 1);
		assertNotNull("Cluster name should not be null", snapshot.clusterName);
		assertTrue("Open connections should be > 0", snapshot.openConnections > 0);
		assertFalse("Nodes list should not be empty", snapshot.nodes.isEmpty());

		MetricsSnapshot.NodeSnapshot nodeSnapshot = snapshot.nodes.get(0);
		assertFalse("Node name should not be empty", nodeSnapshot.nodeName.isEmpty());
		assertTrue("Node should have open connections", nodeSnapshot.openConnections > 0);

		long openConnections = 0;
		long syncInUse = 0;
		long syncInPool = 0;

		for (MetricsSnapshot.NodeSnapshot node : snapshot.nodes) {
			openConnections += node.openConnections;
			syncInUse += node.syncConnections.inUse;
			syncInPool += node.syncConnections.inPool;
		}

		assertTrue("Aggregated snapshot should be present",
			snapshot.clusterAggregated != null);
		assertEquals(openConnections, snapshot.clusterAggregated.openConnections);
		assertEquals(syncInUse, snapshot.clusterAggregated.syncConnections.inUse);
		assertEquals(syncInPool, snapshot.clusterAggregated.syncConnections.inPool);
	}

	@Test
	public void testExtendedMetricsDisabled() throws Exception {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.enableExtendedMetrics = false;
		policy.addExporter(exporter);

		MetricsSnapshot snapshot;
		try {
			client.enableMetrics(policy);
			snapshot = waitForSnapshot(exporter,
				value -> !value.extendedMetricsEnabled);
		}
		finally {
			client.disableMetrics();
		}

		assertNotNull("Exporter should receive a standard metrics snapshot", snapshot);
		assertFalse("Extended flag should be false", snapshot.extendedMetricsEnabled);
		assertEquals("CPU should be 0 when extended disabled", 0.0, snapshot.cpuPercent, 0.001);
		assertEquals("Memory should be 0 when extended disabled", 0, snapshot.memoryBytes);
		assertEquals("Command count should be 0 when extended disabled", 0, snapshot.commandCount);

		for (MetricsSnapshot.NodeSnapshot nodeSnapshot : snapshot.nodes) {
			assertTrue("Namespaces should be empty when extended disabled",
				nodeSnapshot.namespaces.isEmpty());
		}
	}

	@Test
	public void testDynamicConfigReloadsExtendedMetricsSetting() throws Exception {
		final String configProperty = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
		assumeTrue("External client config takes precedence over the test config",
			System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") == null);

		String originalConfig = System.getProperty(configProperty);
		Path configFile = Files.createTempFile("aerospike-metrics-", ".yaml");
		IAerospikeClient configuredClient = null;

		try {
			writeMetricsConfig(configFile, true);
			System.setProperty(configProperty, configFile.toUri().toString());

			ClientPolicy cp = new ClientPolicy();
			cp.user = args.user;
			cp.password = args.password;
			cp.authMode = args.authMode;
			cp.tlsPolicy = args.tlsPolicy;
			configuredClient = new AerospikeClient(
				cp, Host.parseHosts(args.host, args.port));

			RecordingExporter exporter = new RecordingExporter();
			MetricsPolicy policy = newMetricsPolicy();
			policy.addExporter(exporter);
			configuredClient.enableMetrics(policy);

			assertNotNull("Expected an extended metrics snapshot",
				waitForSnapshot(exporter, snapshot -> snapshot.extendedMetricsEnabled));

			writeMetricsConfig(configFile, false);
			Files.setLastModifiedTime(configFile,
				FileTime.fromMillis(System.currentTimeMillis() + 2000));

			assertNotNull("Expected dynamic config to restart metrics in standard mode",
				waitForSnapshot(exporter, snapshot -> !snapshot.extendedMetricsEnabled));
		}
		finally {
			if (configuredClient != null) {
				configuredClient.close();
			}
			Files.deleteIfExists(configFile);

			if (originalConfig == null) {
				System.clearProperty(configProperty);
			}
			else {
				System.setProperty(configProperty, originalConfig);
			}
		}
	}

	@Test
	public void testDisableMetricsStopsExporter() throws Exception {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.addExporter(exporter);

		try {
			client.enableMetrics(policy);
			assertNotNull("Should receive a snapshot before disable",
				waitForSnapshot(exporter, value -> true));
		}
		finally {
			client.disableMetrics();
		}

		int countAfterDisable = exporter.snapshots.size();
		Thread.sleep(1500);
		assertEquals("No new snapshots after disable",
			countAfterDisable, exporter.snapshots.size());
	}

	@Test
	public void testEnableDisableEnableUsesNewExporter() throws Exception {
		RecordingExporter first = new RecordingExporter();
		MetricsPolicy firstPolicy = newMetricsPolicy();
		firstPolicy.addExporter(first);

		try {
			client.enableMetrics(firstPolicy);
			assertNotNull("First exporter should receive a snapshot",
				waitForSnapshot(first, value -> true));
		}
		finally {
			client.disableMetrics();
		}

		int firstCount = first.snapshots.size();
		RecordingExporter second = new RecordingExporter();
		MetricsPolicy secondPolicy = newMetricsPolicy();
		secondPolicy.addExporter(second);

		try {
			client.enableMetrics(secondPolicy);
			assertNotNull("Second exporter should receive a snapshot",
				waitForSnapshot(second, value -> true));
		}
		finally {
			client.disableMetrics();
		}

		assertEquals("Disabled exporter should not receive later snapshots",
			firstCount, first.snapshots.size());
	}

	@Test
	public void testClientsKeepIndependentSettingsWithSharedExporter()
		throws Exception {
		RecordingExporter exporter = new RecordingExporter();
		IAerospikeClient extendedClient = newClient("metrics-extended");
		IAerospikeClient standardClient = newClient("metrics-standard");

		try {
			MetricsPolicy extendedPolicy = newMetricsPolicy();
			extendedPolicy.addExporter(exporter);

			MetricsPolicy standardPolicy = newMetricsPolicy();
			standardPolicy.enableExtendedMetrics = false;
			standardPolicy.addExporter(exporter);

			extendedClient.enableMetrics(extendedPolicy);
			standardClient.enableMetrics(standardPolicy);

			assertNotNull("Expected extended snapshot from first client",
				waitForSnapshot(exporter, snapshot ->
					"metrics-extended".equals(snapshot.appId)
						&& snapshot.extendedMetricsEnabled));
			MetricsSnapshot standardSnapshot = waitForSnapshot(
				exporter,
				snapshot -> "metrics-standard".equals(snapshot.appId)
					&& !snapshot.extendedMetricsEnabled
			);
			assertNotNull("Expected standard snapshot from second client",
				standardSnapshot);
			assertEquals(0.0, standardSnapshot.cpuPercent, 0.001);
			assertEquals(0, standardSnapshot.memoryBytes);
		}
		finally {
			extendedClient.close();
			standardClient.close();
		}
	}

	@Test
	public void testClientClosedWhileMetricsEnabled() throws Exception {
		RecordingExporter exporter = new RecordingExporter();
		IAerospikeClient tempClient = newClient("metrics-close");

		try {
			MetricsPolicy policy = newMetricsPolicy();
			policy.addExporter(exporter);
			tempClient.enableMetrics(policy);
			assertNotNull("Should receive a snapshot before close",
				waitForSnapshot(exporter, value -> true));

			tempClient.close();
			int countAfterClose = exporter.snapshots.size();
			Thread.sleep(1500);
			assertEquals("No new snapshots after client close",
				countAfterClose, exporter.snapshots.size());
		}
		finally {
			tempClient.close();
		}
	}

	private static IAerospikeClient newClient(String appId) {
		ClientPolicy policy = new ClientPolicy();
		policy.user = args.user;
		policy.password = args.password;
		policy.authMode = args.authMode;
		policy.tlsPolicy = args.tlsPolicy;
		policy.appId = appId;
		return new AerospikeClient(
			policy, Host.parseHosts(args.host, args.port));
	}

	private static MetricsPolicy newMetricsPolicy() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.interval = 1;
		policy.reportDir = "target/metrics-tests";
		return policy;
	}

	private static void writeMetricsConfig(Path path, boolean enableExtendedMetrics)
		throws Exception {
		String yaml = "version: 1.0.0\n"
			+ "static:\n"
			+ "  client:\n"
			+ "    config_interval: 1000\n"
			+ "dynamic:\n"
			+ "  metrics:\n"
			+ "    enable: true\n"
			+ "    enable_extended_metrics: " + enableExtendedMetrics + "\n";
		Files.writeString(path, yaml);
	}

	private static MetricsSnapshot waitForSnapshot(
		RecordingExporter exporter,
		Predicate<MetricsSnapshot> predicate
	) throws InterruptedException {
		long deadline = System.currentTimeMillis() + SNAPSHOT_TIMEOUT_MILLIS;

		while (System.currentTimeMillis() < deadline) {
			synchronized (exporter.snapshots) {
				for (MetricsSnapshot snapshot : exporter.snapshots) {
					if (predicate.test(snapshot)) {
						return snapshot;
					}
				}
			}
			Thread.sleep(50);
		}
		return null;
	}

	private static class RecordingExporter implements IMetricsExporter {
		final List<MetricsSnapshot> snapshots = Collections.synchronizedList(new ArrayList<>());

		@Override
		public void export(MetricsSnapshot snapshot) {
			snapshots.add(snapshot);
		}

		@Override
		public void close() {
		}
	}
}
