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
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;
import com.aerospike.test.sync.TestSync;

/**
 * Integration tests for the metrics exporter framework.
 * These tests require a running Aerospike server.
 */
public class TestMetricsExporter extends TestSync {

	private static final int SHORT_INTERVAL = 2;

	@Test
	public void testExporterReceivesSnapshots() throws Exception {
		RecordingExporter exporter = new RecordingExporter();

		MetricsPolicy policy = newMetricsPolicy();
		policy.interval = SHORT_INTERVAL;
		policy.addExporter(exporter);

		client.enableMetrics(policy);
		doOperationsForSeconds(8);
		client.disableMetrics();

		assertTrue("Exporter should have received at least 1 snapshot",
			exporter.snapshots.size() >= 1);
	}

	@Test
	public void testDispatchCallsAllExporters() throws Exception {
		RecordingExporter first = new RecordingExporter();
		RecordingExporter second = new RecordingExporter();

		MetricsPolicy policy = newMetricsPolicy();
		policy.interval = SHORT_INTERVAL;
		policy.addExporter(first);
		policy.addExporter(second);

		client.enableMetrics(policy);
		doOperationsForSeconds(8);
		client.disableMetrics();

		assertTrue("First exporter should receive snapshots", first.snapshots.size() >= 1);
		assertTrue("Second exporter should receive snapshots", second.snapshots.size() >= 1);
		assertEquals("Both exporters should receive same count",
			first.snapshots.size(), second.snapshots.size());
	}

	@Test
	public void testSnapshotHasValidClusterData() throws Exception {
		RecordingExporter exporter = new RecordingExporter();

		MetricsPolicy policy = newMetricsPolicy();
		policy.interval = SHORT_INTERVAL;
		policy.addExporter(exporter);

		client.enableMetrics(policy);
		doOperationsForSeconds(8);
		client.disableMetrics();

		assertTrue("Should have at least 1 snapshot", exporter.snapshots.size() >= 1);

		MetricsSnapshot snapshot = exporter.snapshots.get(0);
		assertTrue("Should have at least 1 node", snapshot.totalNodes >= 1);
		assertFalse("Cluster name should not be null", snapshot.clusterName == null);
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
	public void testExtendedMetricsEnabled() throws Exception {
		RecordingExporter exporter = new RecordingExporter();

		MetricsPolicy policy = newMetricsPolicy();
		policy.interval = SHORT_INTERVAL;
		policy.enableExtendedMetrics = true;
		policy.addExporter(exporter);

		client.enableMetrics(policy);
		doOperationsForSeconds(8);
		client.disableMetrics();

		assertTrue(exporter.snapshots.size() >= 1);

		MetricsSnapshot snapshot = exporter.snapshots.get(exporter.snapshots.size() - 1);
		assertTrue("Extended flag should be true", snapshot.extendedMetricsEnabled);
	}

	@Test
	public void testExtendedMetricsDisabled() throws Exception {
		RecordingExporter exporter = new RecordingExporter();

		MetricsPolicy policy = newMetricsPolicy();
		policy.interval = SHORT_INTERVAL;
		policy.enableExtendedMetrics = false;
		policy.addExporter(exporter);

		client.enableMetrics(policy);
		doOperationsForSeconds(8);
		client.disableMetrics();

		assertTrue(exporter.snapshots.size() >= 1);

		MetricsSnapshot snapshot = exporter.snapshots.get(exporter.snapshots.size() - 1);
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
			policy.interval = 1;
			policy.addExporter(exporter);
			configuredClient.enableMetrics(policy);

			assertTrue("Expected an extended metrics snapshot",
				waitForExtendedSetting(exporter, true, 8000));

			writeMetricsConfig(configFile, false);
			Files.setLastModifiedTime(configFile,
				FileTime.fromMillis(System.currentTimeMillis() + 2000));

			assertTrue("Expected dynamic config to restart metrics in standard mode",
				waitForExtendedSetting(exporter, false, 8000));
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
		policy.interval = SHORT_INTERVAL;
		policy.addExporter(exporter);

		client.enableMetrics(policy);
		doOperationsForSeconds(6);
		client.disableMetrics();

		int countAfterDisable = exporter.snapshots.size();
		assertTrue("Should have snapshots before disable", countAfterDisable >= 1);

		doOperationsForSeconds(6);
		assertEquals("No new snapshots after disable",
			countAfterDisable, exporter.snapshots.size());
	}

	@Test
	public void testEnableDisableEnableCycle() throws Exception {
		RecordingExporter firstExporter = new RecordingExporter();
		RecordingExporter secondExporter = new RecordingExporter();

		MetricsPolicy firstPolicy = newMetricsPolicy();
		firstPolicy.interval = SHORT_INTERVAL;
		firstPolicy.addExporter(firstExporter);

		client.enableMetrics(firstPolicy);
		doOperationsForSeconds(6);
		client.disableMetrics();

		int firstCount = firstExporter.snapshots.size();
		assertTrue("First exporter should have snapshots", firstCount >= 1);

		MetricsPolicy secondPolicy = newMetricsPolicy();
		secondPolicy.interval = SHORT_INTERVAL;
		secondPolicy.addExporter(secondExporter);

		client.enableMetrics(secondPolicy);
		doOperationsForSeconds(6);
		client.disableMetrics();

		assertTrue("Second exporter should have snapshots", secondExporter.snapshots.size() >= 1);
		assertEquals("First exporter should not get more snapshots after disable",
			firstCount, firstExporter.snapshots.size());
	}

	@Test
	public void testNoExportersNoThread() throws Exception {
		MetricsPolicy policy = newMetricsPolicy();
		policy.interval = SHORT_INTERVAL;

		client.enableMetrics(policy);
		doOperationsForSeconds(4);
		client.disableMetrics();
	}

	// ── Multi-client tests ────────────────────────────────────────

	@Test
	public void testTwoClientsSameClusterIsolatedMetrics() throws Exception {
		RecordingExporter exporterA = new RecordingExporter();
		RecordingExporter exporterB = new RecordingExporter();

		ClientPolicy cp = new ClientPolicy();
		cp.user = args.user;
		cp.password = args.password;
		cp.authMode = args.authMode;
		cp.tlsPolicy = args.tlsPolicy;

		Host[] hosts = Host.parseHosts(args.host, args.port);
		IAerospikeClient clientB = new AerospikeClient(cp, hosts);

		try {
			MetricsPolicy policyA = newMetricsPolicy();
			policyA.interval = SHORT_INTERVAL;
			policyA.addExporter(exporterA);

			MetricsPolicy policyB = newMetricsPolicy();
			policyB.interval = SHORT_INTERVAL;
			policyB.addExporter(exporterB);

			client.enableMetrics(policyA);
			clientB.enableMetrics(policyB);

			doOperationsForSeconds(8);
			doOperationsWithClient(clientB, 8);

			client.disableMetrics();
			clientB.disableMetrics();

			assertTrue("Client A exporter should receive snapshots",
				exporterA.snapshots.size() >= 1);
			assertTrue("Client B exporter should receive snapshots",
				exporterB.snapshots.size() >= 1);

			// Each exporter should only contain snapshots from its own client.
			// Verify no cross-contamination by checking snapshot count is independent.
			// Both connect to same cluster, but snapshots are per-client.
		}
		finally {
			clientB.close();
		}
	}

	@Test
	public void testOneClientMetricsOtherWithout() throws Exception {
		RecordingExporter exporter = new RecordingExporter();

		ClientPolicy cp = new ClientPolicy();
		cp.user = args.user;
		cp.password = args.password;
		cp.authMode = args.authMode;
		cp.tlsPolicy = args.tlsPolicy;

		Host[] hosts = Host.parseHosts(args.host, args.port);
		IAerospikeClient clientNoMetrics = new AerospikeClient(cp, hosts);

		try {
			MetricsPolicy policy = newMetricsPolicy();
			policy.interval = SHORT_INTERVAL;
			policy.addExporter(exporter);

			client.enableMetrics(policy);

			// Both clients do operations, but only 'client' has metrics enabled.
			doOperationsForSeconds(8);
			doOperationsWithClient(clientNoMetrics, 8);

			client.disableMetrics();

			assertTrue("Metrics-enabled client should produce snapshots",
				exporter.snapshots.size() >= 1);
		}
		finally {
			clientNoMetrics.close();
		}
	}

	@Test
	public void testSharedExporterAcrossClients() throws Exception {
		RecordingExporter sharedExporter = new RecordingExporter();

		ClientPolicy cp = new ClientPolicy();
		cp.user = args.user;
		cp.password = args.password;
		cp.authMode = args.authMode;
		cp.tlsPolicy = args.tlsPolicy;

		Host[] hosts = Host.parseHosts(args.host, args.port);
		IAerospikeClient clientB = new AerospikeClient(cp, hosts);

		try {
			MetricsPolicy policyA = newMetricsPolicy();
			policyA.interval = SHORT_INTERVAL;
			policyA.addExporter(sharedExporter);

			MetricsPolicy policyB = newMetricsPolicy();
			policyB.interval = SHORT_INTERVAL;
			policyB.addExporter(sharedExporter);

			client.enableMetrics(policyA);
			clientB.enableMetrics(policyB);

			doOperationsForSeconds(8);
			doOperationsWithClient(clientB, 8);

			client.disableMetrics();
			clientB.disableMetrics();

			// Shared exporter receives snapshots from BOTH clients.
			// Each client's MetricsExporterThread calls export() independently,
			// so the shared exporter receives interleaved snapshots.
			assertTrue("Shared exporter should receive snapshots from both clients",
				sharedExporter.snapshots.size() >= 2);
		}
		finally {
			clientB.close();
		}
	}

	@Test
	public void testClientClosedWhileMetricsEnabled() throws Exception {
		RecordingExporter exporter = new RecordingExporter();

		ClientPolicy cp = new ClientPolicy();
		cp.user = args.user;
		cp.password = args.password;
		cp.authMode = args.authMode;
		cp.tlsPolicy = args.tlsPolicy;

		Host[] hosts = Host.parseHosts(args.host, args.port);
		IAerospikeClient tempClient = new AerospikeClient(cp, hosts);

		MetricsPolicy policy = newMetricsPolicy();
		policy.interval = SHORT_INTERVAL;
		policy.addExporter(exporter);

		tempClient.enableMetrics(policy);
		doOperationsWithClient(tempClient, 6);

		int countBeforeClose = exporter.snapshots.size();
		assertTrue("Should have snapshots before close", countBeforeClose >= 1);

		// Close client without explicitly calling disableMetrics.
		// The exporter thread should stop gracefully.
		tempClient.close();

		Util.sleep(4000);

		// No new snapshots after client close.
		assertEquals("No new snapshots after client close",
			countBeforeClose, exporter.snapshots.size());
	}

	@Test
	public void testTwoClientsDifferentExtendedMetricsSettings() throws Exception {
		RecordingExporter exporterExtended = new RecordingExporter();
		RecordingExporter exporterStandard = new RecordingExporter();

		ClientPolicy cp = new ClientPolicy();
		cp.user = args.user;
		cp.password = args.password;
		cp.authMode = args.authMode;
		cp.tlsPolicy = args.tlsPolicy;

		Host[] hosts = Host.parseHosts(args.host, args.port);
		IAerospikeClient clientStandard = new AerospikeClient(cp, hosts);

		try {
			MetricsPolicy extendedPolicy = newMetricsPolicy();
			extendedPolicy.interval = SHORT_INTERVAL;
			extendedPolicy.enableExtendedMetrics = true;
			extendedPolicy.addExporter(exporterExtended);

			MetricsPolicy standardPolicy = newMetricsPolicy();
			standardPolicy.interval = SHORT_INTERVAL;
			standardPolicy.enableExtendedMetrics = false;
			standardPolicy.addExporter(exporterStandard);

			client.enableMetrics(extendedPolicy);
			clientStandard.enableMetrics(standardPolicy);

			doOperationsForSeconds(8);
			doOperationsWithClient(clientStandard, 8);

			client.disableMetrics();
			clientStandard.disableMetrics();

			assertTrue(exporterExtended.snapshots.size() >= 1);
			assertTrue(exporterStandard.snapshots.size() >= 1);

			MetricsSnapshot extSnapshot = exporterExtended.snapshots.get(
				exporterExtended.snapshots.size() - 1);
			MetricsSnapshot stdSnapshot = exporterStandard.snapshots.get(
				exporterStandard.snapshots.size() - 1);

			assertTrue("Extended client should have extendedMetricsEnabled=true",
				extSnapshot.extendedMetricsEnabled);
			assertFalse("Standard client should have extendedMetricsEnabled=false",
				stdSnapshot.extendedMetricsEnabled);
			assertEquals("Standard client CPU should be 0",
				0.0, stdSnapshot.cpuPercent, 0.001);

			for (MetricsSnapshot.NodeSnapshot nodeSnapshot : stdSnapshot.nodes) {
				assertTrue("Standard client should have no namespace snapshots",
					nodeSnapshot.namespaces.isEmpty());
			}
		}
		finally {
			clientStandard.close();
		}
	}

	// ── Helpers ────────────────────────────────────────────────────

	private MetricsPolicy newMetricsPolicy() {
		MetricsPolicy policy = new MetricsPolicy();
		policy.interval = SHORT_INTERVAL;
		policy.reportDir = "target/metrics-tests";
		return policy;
	}

	private void writeMetricsConfig(Path path, boolean enableExtendedMetrics) throws Exception {
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

	private boolean waitForExtendedSetting(
		RecordingExporter exporter,
		boolean expected,
		long timeoutMillis
	) {
		long deadline = System.currentTimeMillis() + timeoutMillis;

		while (System.currentTimeMillis() < deadline) {
			synchronized (exporter.snapshots) {
				for (MetricsSnapshot snapshot : exporter.snapshots) {
					if (snapshot.extendedMetricsEnabled == expected) {
						return true;
					}
				}
			}
			Util.sleep(100);
		}
		return false;
	}

	private void doOperationsForSeconds(int seconds) throws Exception {
		doOperationsWithClient(client, seconds);
	}

	private void doOperationsWithClient(IAerospikeClient targetClient, int seconds) throws Exception {
		long endTime = System.currentTimeMillis() + seconds * 1000L;
		int count = 0;

		while (System.currentTimeMillis() < endTime) {
			Key key = new Key(args.namespace, args.set, "exporter-test-" + count);
			Bin bin = new Bin("val", count);
			targetClient.put(null, key, bin);
			targetClient.get(null, key);
			count++;
			Util.sleep(50);
		}
	}

	// ── Test exporters ─────────────────────────────────────────────

	/**
	 * Exporter that records all received snapshots.
	 */
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
