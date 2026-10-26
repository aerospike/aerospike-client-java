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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Log;
import com.aerospike.client.metrics.IMetricsExporter;
import com.aerospike.client.metrics.MetricsExporterType;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsSnapshot;
import com.aerospike.client.metrics.NodeMetricsSnapshot;
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
		assertTrue("Periodic snapshot should report metrics enabled",
			snapshot.metricsEnabled);
		assertTrue("Operational metrics should be enabled by default",
			snapshot.operationalMetricsEnabled);
		assertTrue("Should have at least 1 node", snapshot.totalNodes >= 1);
		assertNotNull("Cluster name should not be null", snapshot.clusterName);
		assertTrue("Open connections should be > 0", snapshot.openConnections > 0);
		assertFalse("Nodes list should not be empty", snapshot.nodes.isEmpty());

		NodeMetricsSnapshot nodeSnapshot = snapshot.nodes.get(0);
		assertFalse("Node name should not be empty", nodeSnapshot.nodeName.isEmpty());
		assertTrue("Node should have open connections", nodeSnapshot.openConnections > 0);

		long openConnections = 0;
		long syncInUse = 0;
		long syncInPool = 0;

		for (NodeMetricsSnapshot node : snapshot.nodes) {
			openConnections += node.openConnections;
			syncInUse += node.syncConnections.inUse;
			syncInPool += node.syncConnections.inPool;
		}

		assertEquals(openConnections, snapshot.openConnections);
		assertTrue(syncInUse + syncInPool > 0);
	}

	@Test
	public void testTwoCustomExportersReceiveOperationalAndUsageSnapshots()
		throws Exception {
		RecordingExporter first = new RecordingExporter();
		RecordingExporter second = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.exportInterval = Duration.ofMillis(100);
		policy.exporter = MetricsExporterType.CUSTOM;
		policy.enableOperationalMetrics = true;
		policy.enableUsageMetrics = true;
		policy.addExporter(first);
		policy.addExporter(second);

		MetricsSnapshot snapshot;
		try {
			client.enableMetrics(policy);
			snapshot = waitForSnapshot(first, value -> value.metricsEnabled);
			assertNotNull("First custom exporter did not receive a periodic snapshot",
				snapshot);
			assertNotNull("Second custom exporter did not receive a periodic snapshot",
				waitForSnapshot(second, value -> value.metricsEnabled));
		}
		finally {
			client.disableMetrics();
		}

		assertTrue(snapshot.operationalMetricsEnabled);
		assertTrue(snapshot.usageMetricsEnabled);
		assertFalse(snapshot.nodes.isEmpty());
		assertEquals(first.snapshots.size(), second.snapshots.size());
		assertTrue("Expected a periodic and final snapshot",
			first.snapshots.size() >= 2);

		for (int i = 0; i < first.snapshots.size(); i++) {
			assertTrue("Exporters did not receive the same snapshot instance",
				first.snapshots.get(i) == second.snapshots.get(i));
		}
		assertFalse(first.snapshots.get(first.snapshots.size() - 1).metricsEnabled);
	}

	@Test
	public void testOperationalMetricsDisabled() throws Exception {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.enableOperationalMetrics = false;
		policy.addExporter(exporter);

		MetricsSnapshot snapshot;
		try {
			client.enableMetrics(policy);
			snapshot = waitForSnapshot(exporter,
				value -> !value.operationalMetricsEnabled);
		}
		finally {
			client.disableMetrics();
		}

		assertNotNull("Exporter should receive a standard metrics snapshot", snapshot);
		assertFalse("Operational flag should be false", snapshot.operationalMetricsEnabled);
		assertEquals("CPU should be 0 when operational metrics are disabled",
			0.0, snapshot.cpuPercent, 0.001);
		assertEquals("Memory should be 0 when operational metrics are disabled",
			0, snapshot.memoryBytes);
		assertEquals("Command count should be 0 when operational metrics are disabled",
			0, snapshot.commandCount);

		for (NodeMetricsSnapshot nodeSnapshot : snapshot.nodes) {
			assertTrue("Namespaces should be empty when operational metrics are disabled",
				nodeSnapshot.namespaces.isEmpty());
		}
	}

	@Test
	public void testDynamicConfigReloadsOperationalMetricsSetting() throws Exception {
		final String configProperty = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
		assumeTrue("External client config takes precedence over the test config",
			System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") == null);

		String originalConfig = System.getProperty(configProperty);
		Path configFile = Files.createTempFile("aerospike-metrics-", ".yaml");
		IAerospikeClient configuredClient = null;

		try {
			writeMetricsConfig(configFile, true, true);
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

			assertNotNull("Expected an operational metrics snapshot",
				waitForSnapshot(exporter, snapshot -> snapshot.operationalMetricsEnabled));
			String initialThreadName = exporter.threadNames.get(0);

			writeMetricsConfig(configFile, true, true);
			Files.setLastModifiedTime(configFile,
				FileTime.fromMillis(System.currentTimeMillis() + 2000));
			Thread.sleep(1500);

			synchronized (exporter.threadNames) {
				for (String threadName : exporter.threadNames) {
					assertEquals("No-op reload replaced the metrics runtime",
						initialThreadName, threadName);
				}
			}

			writeMetricsConfig(configFile, true, false);
			Files.setLastModifiedTime(configFile,
				FileTime.fromMillis(System.currentTimeMillis() + 4000));

			assertNotNull("Expected final snapshot from the previous metrics runtime",
				waitForSnapshot(exporter, snapshot ->
					!snapshot.metricsEnabled && snapshot.operationalMetricsEnabled));
			assertNotNull("Expected dynamic config to restart metrics in standard mode",
				waitForSnapshot(exporter, snapshot ->
					snapshot.metricsEnabled && !snapshot.operationalMetricsEnabled));
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
	public void testDynamicDisableWinsOverOperationalRestart() throws Exception {
		final String configProperty = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
		assumeTrue("External client config takes precedence over the test config",
			System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") == null);

		String originalConfig = System.getProperty(configProperty);
		Path configFile = Files.createTempFile("aerospike-metrics-disable-", ".yaml");
		IAerospikeClient configuredClient = null;

		try {
			writeMetricsConfig(configFile, true, true);
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

			assertNotNull("Expected metrics before dynamic disable",
				waitForSnapshot(exporter, snapshot -> snapshot.metricsEnabled));

			writeMetricsConfig(configFile, false, false);
			Files.setLastModifiedTime(configFile,
				FileTime.fromMillis(System.currentTimeMillis() + 2000));

			assertNotNull("Expected final disabled snapshot",
				waitForSnapshot(exporter, snapshot -> !snapshot.metricsEnabled));
			int countAfterDisable = exporter.snapshots.size();
			Thread.sleep(1500);
			assertEquals("Dynamic disable started a replacement metrics thread",
				countAfterDisable, exporter.snapshots.size());
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
	public void testInvalidDynamicPolicyKeepsCurrentMetricsRuntime() throws Exception {
		final String configProperty = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
		assumeTrue("External client config takes precedence over the test config",
			System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") == null);

		String originalConfig = System.getProperty(configProperty);
		Path configFile = Files.createTempFile("aerospike-metrics-invalid-", ".yaml");
		IAerospikeClient configuredClient = null;

		try {
			writeMetricsConfig(configFile, true, true);
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
			policy.exportInterval = Duration.ofMillis(200);
			policy.addExporter(exporter);
			configuredClient.enableMetrics(policy);

			assertNotNull("Expected metrics before invalid reload",
				waitForSnapshot(exporter, snapshot -> snapshot.metricsEnabled));
			String threadName = exporter.threadNames.get(0);

			writeMetricsConfig(configFile, true, true, 0);
			Files.setLastModifiedTime(configFile,
				FileTime.fromMillis(System.currentTimeMillis() + 2000));
			Thread.sleep(1500);

			int countAfterReload = exporter.snapshots.size();
			assertTrue("Current metrics runtime stopped after invalid reload",
				waitForSnapshotCount(exporter, countAfterReload + 1));

			synchronized (exporter.threadNames) {
				for (String currentThreadName : exporter.threadNames) {
					assertEquals("Invalid policy replaced the metrics runtime",
						threadName, currentThreadName);
				}
			}
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
	public void testDisablePerformsFinalExportOnMetricsThread() {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.exportInterval = Duration.ofHours(1);
		policy.addExporter(exporter);
		String callerThread = Thread.currentThread().getName();

		client.enableMetrics(policy);
		client.disableMetrics();

		assertEquals(1, exporter.snapshots.size());
		assertFalse(exporter.snapshots.get(0).metricsEnabled);
		assertNotEquals(callerThread, exporter.threadNames.get(0));
		assertTrue(exporter.threadNames.get(0).startsWith("aerospike-metrics-"));
	}

	@Test
	public void testReenableRetainsSameCustomExporter() {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.exportInterval = Duration.ofHours(1);
		policy.addExporter(exporter);

		client.enableMetrics(policy);
		client.disableMetrics();
		client.enableMetrics(policy);
		client.disableMetrics();

		assertEquals(2, exporter.snapshots.size());
		assertNotEquals(exporter.threadNames.get(0), exporter.threadNames.get(1));
	}

	@Test
	public void testDisableWaitsForActiveExportBeforeFinalExport() throws Exception {
		BlockingExporter exporter = new BlockingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.exportInterval = Duration.ofMillis(10);
		policy.addExporter(exporter);
		CountDownLatch disableComplete = new CountDownLatch(1);
		AtomicBoolean callerInterruptRestored = new AtomicBoolean();
		Thread disableThread = new Thread(() -> {
			try {
				client.disableMetrics();
			}
			finally {
				callerInterruptRestored.set(Thread.currentThread().isInterrupted());
				disableComplete.countDown();
			}
		}, "metrics-disable-test");

		client.enableMetrics(policy);
		try {
			assertTrue("Periodic export did not start",
				exporter.firstExportStarted.await(
					SNAPSHOT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));

			disableThread.start();
			assertFalse("Disable returned before the active export completed",
				disableComplete.await(100, TimeUnit.MILLISECONDS));
			disableThread.interrupt();
			assertFalse("Interrupted disable returned before the active export completed",
				disableComplete.await(100, TimeUnit.MILLISECONDS));
			assertFalse("Active exporter was interrupted", exporter.interrupted.get());
		}
		finally {
			exporter.allowFirstExportToComplete.countDown();

			if (disableThread.getState() != Thread.State.NEW) {
				disableThread.join(SNAPSHOT_TIMEOUT_MILLIS);
			}
			client.disableMetrics();
		}

		assertEquals("Disable did not complete", 0, disableComplete.getCount());
		assertEquals(2, exporter.callCount.get());
		assertTrue("Final export started before the periodic export completed",
			exporter.finalExportFollowedFirst.get());
		assertFalse("Periodic and final exports overlapped", exporter.overlap.get());
		assertEquals(exporter.threadNames.get(0), exporter.threadNames.get(1));
		assertEquals(Boolean.TRUE, exporter.metricsEnabledValues.get(0));
		assertEquals(Boolean.FALSE, exporter.metricsEnabledValues.get(1));
		assertTrue("Disable caller interrupt status was not restored",
			callerInterruptRestored.get());
	}

	@Test
	public void testClientsUseDistinctMetricsThreadNames() {
		RecordingExporter firstExporter = new RecordingExporter();
		RecordingExporter secondExporter = new RecordingExporter();
		IAerospikeClient firstClient = newClient("metrics-thread-first");
		IAerospikeClient secondClient = newClient("metrics-thread-second");

		try {
			MetricsPolicy firstPolicy = newMetricsPolicy();
			firstPolicy.exportInterval = Duration.ofHours(1);
			firstPolicy.addExporter(firstExporter);
			MetricsPolicy secondPolicy = newMetricsPolicy();
			secondPolicy.exportInterval = Duration.ofHours(1);
			secondPolicy.addExporter(secondExporter);

			firstClient.enableMetrics(firstPolicy);
			secondClient.enableMetrics(secondPolicy);
			firstClient.disableMetrics();
			secondClient.disableMetrics();

			assertNotEquals(
				firstExporter.threadNames.get(0),
				secondExporter.threadNames.get(0));
		}
		finally {
			firstClient.close();
			secondClient.close();
		}
	}

	@Test
	public void testRepeatedDisableAndCloseDoNotExportAgain() {
		RecordingExporter exporter = new RecordingExporter();
		IAerospikeClient tempClient = newClient("metrics-repeat-disable");

		try {
			MetricsPolicy policy = newMetricsPolicy();
			policy.exportInterval = Duration.ofHours(1);
			policy.addExporter(exporter);

			tempClient.enableMetrics(policy);
			tempClient.disableMetrics();
			tempClient.disableMetrics();
			tempClient.close();

			assertEquals(1, exporter.snapshots.size());
		}
		finally {
			tempClient.close();
		}
	}

	@Test
	public void testNoneExporterProducesNoCallbacks() {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.addExporter(exporter);
		policy.exporter = MetricsExporterType.NONE;

		client.enableMetrics(policy);
		client.disableMetrics();

		assertTrue(exporter.snapshots.isEmpty());
	}

	@Test
	public void testEmptyFileDirectoryProducesNoCallbacks() {
		RecordingExporter exporter = new RecordingExporter();
		MetricsPolicy policy = newMetricsPolicy();
		policy.addExporter(exporter);
		policy.exporter = MetricsExporterType.FILE;
		policy.reportDir = "";

		client.enableMetrics(policy);
		client.disableMetrics();

		assertTrue(exporter.snapshots.isEmpty());
	}

	@Test
	public void testLifecycleLogsContainThreadAndClusterIdentity() {
		List<String> messages = Collections.synchronizedList(new ArrayList<>());
		RecordingExporter exporter = new RecordingExporter();
		IAerospikeClient tempClient = newClient("metrics-lifecycle-log");

		Log.setCallback((level, message) -> {
			if (level == Log.Level.INFO) {
				messages.add(message);
			}
		});

		try {
			MetricsPolicy policy = newMetricsPolicy();
			policy.exportInterval = Duration.ofHours(1);
			policy.addExporter(exporter);

			tempClient.enableMetrics(policy);
			tempClient.disableMetrics();

			String threadName = exporter.threadNames.get(0);
			String clusterName = exporter.snapshots.get(0).clusterName;
			assertTrue(messages.contains("Metrics thread " + threadName +
				" started for cluster " + clusterName +
				", interval=" + policy.exportInterval));
			assertTrue(messages.contains("Metrics thread " + threadName +
				" stopped for cluster " + clusterName));
		}
		finally {
			Log.setCallback(null);
			tempClient.close();
		}
	}

	@Test
	public void testFileExporterWritesFinalSnapshotBeforeClose() throws Exception {
		Path directory = Files.createTempDirectory("metrics-final-export-");

		try {
			MetricsPolicy policy = newMetricsPolicy();
			policy.exportInterval = Duration.ofHours(1);
			policy.reportDir = directory.toString();

			client.enableMetrics(policy);
			client.disableMetrics();

			Path output;
			try (java.util.stream.Stream<Path> files = Files.list(directory)) {
				output = files.findFirst().orElseThrow();
			}
			List<String> lines = Files.readAllLines(output);
			assertEquals("Expected one header and one final snapshot", 2, lines.size());
		}
		finally {
			try (java.util.stream.Stream<Path> files = Files.list(directory)) {
				files.forEach(path -> {
					try {
						Files.delete(path);
					}
					catch (Exception ignored) {
					}
				});
			}
			Files.delete(directory);
		}
	}

	@Test
	public void testFinalExportFailureDoesNotFailDisable() {
		AtomicInteger calls = new AtomicInteger();
		MetricsPolicy policy = newMetricsPolicy();
		policy.exportInterval = Duration.ofHours(1);
		policy.addExporter(snapshot -> {
			calls.incrementAndGet();
			throw new RuntimeException("expected final export failure");
		});

		client.enableMetrics(policy);
		client.disableMetrics();

		assertEquals(1, calls.get());
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
		IAerospikeClient operationalClient = newClient("metrics-operational");
		IAerospikeClient standardClient = newClient("metrics-standard");

		try {
			MetricsPolicy operationalPolicy = newMetricsPolicy();
			operationalPolicy.addExporter(exporter);

			MetricsPolicy standardPolicy = newMetricsPolicy();
			standardPolicy.enableOperationalMetrics = false;
			standardPolicy.addExporter(exporter);

			operationalClient.enableMetrics(operationalPolicy);
			standardClient.enableMetrics(standardPolicy);

			assertNotNull("Expected operational snapshot from first client",
				waitForSnapshot(exporter, snapshot ->
					"metrics-operational".equals(snapshot.appId)
						&& snapshot.operationalMetricsEnabled));
			MetricsSnapshot standardSnapshot = waitForSnapshot(
				exporter,
				snapshot -> "metrics-standard".equals(snapshot.appId)
					&& !snapshot.operationalMetricsEnabled
			);
			assertNotNull("Expected standard snapshot from second client",
				standardSnapshot);
			assertEquals(0.0, standardSnapshot.cpuPercent, 0.001);
			assertEquals(0, standardSnapshot.memoryBytes);
		}
		finally {
			operationalClient.close();
			standardClient.close();
		}
	}

	@Test
	public void testClientClosedWhileMetricsEnabled() throws Exception {
		RecordingExporter exporter = new RecordingExporter();
		IAerospikeClient tempClient = newClient("metrics-close");

		try {
			MetricsPolicy policy = newMetricsPolicy();
			policy.exportInterval = Duration.ofHours(1);
			policy.addExporter(exporter);
			tempClient.enableMetrics(policy);
			tempClient.close();

			assertEquals(1, exporter.snapshots.size());
			assertFalse(exporter.snapshots.get(0).metricsEnabled);
			assertTrue(exporter.threadNames.get(0).startsWith("aerospike-metrics-"));
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
		policy.exportInterval = Duration.ofSeconds(1);
		policy.reportDir = "target/metrics-tests";
		return policy;
	}

	private static void writeMetricsConfig(
		Path path,
		boolean metricsEnabled,
		boolean enableOperationalMetrics
	)
		throws Exception {
		writeMetricsConfig(path, metricsEnabled, enableOperationalMetrics, null);
	}

	private static void writeMetricsConfig(
		Path path,
		boolean metricsEnabled,
		boolean enableOperationalMetrics,
		Integer latencyColumns
	)
		throws Exception {
		String yaml = "version: 1.2.0\n"
			+ "static:\n"
			+ "  client:\n"
			+ "    config_interval: 1000\n"
			+ "dynamic:\n"
			+ "  metrics:\n"
			+ "    enabled: " + metricsEnabled + "\n"
			+ "    extended:\n"
			+ "      operational:\n"
			+ "        enabled: " + enableOperationalMetrics + "\n"
			+ (latencyColumns == null ? "" :
				"        latency_columns: " + latencyColumns + "\n");
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

	private static boolean waitForSnapshotCount(
		RecordingExporter exporter,
		int expectedCount
	) throws InterruptedException {
		long deadline = System.currentTimeMillis() + SNAPSHOT_TIMEOUT_MILLIS;

		while (System.currentTimeMillis() < deadline) {
			if (exporter.snapshots.size() >= expectedCount) {
				return true;
			}
			Thread.sleep(50);
		}
		return false;
	}

	private static class RecordingExporter implements IMetricsExporter {
		final List<MetricsSnapshot> snapshots = Collections.synchronizedList(new ArrayList<>());
		final List<String> threadNames = Collections.synchronizedList(new ArrayList<>());

		@Override
		public void export(MetricsSnapshot snapshot) {
			snapshots.add(snapshot);
			threadNames.add(Thread.currentThread().getName());
		}

	}

	private static final class BlockingExporter implements IMetricsExporter {
		final CountDownLatch firstExportStarted = new CountDownLatch(1);
		final CountDownLatch allowFirstExportToComplete = new CountDownLatch(1);
		final List<String> threadNames = Collections.synchronizedList(new ArrayList<>());
		final List<Boolean> metricsEnabledValues =
			Collections.synchronizedList(new ArrayList<>());
		final AtomicInteger callCount = new AtomicInteger();
		final AtomicBoolean exportInProgress = new AtomicBoolean();
		final AtomicBoolean overlap = new AtomicBoolean();
		final AtomicBoolean interrupted = new AtomicBoolean();
		final AtomicBoolean firstExportCompleted = new AtomicBoolean();
		final AtomicBoolean finalExportFollowedFirst = new AtomicBoolean();

		@Override
		public void export(MetricsSnapshot snapshot) {
			if (!exportInProgress.compareAndSet(false, true)) {
				overlap.set(true);
			}

			try {
				int call = callCount.incrementAndGet();
				threadNames.add(Thread.currentThread().getName());
				metricsEnabledValues.add(snapshot.metricsEnabled);

				if (call == 1) {
					firstExportStarted.countDown();

					try {
						if (!allowFirstExportToComplete.await(
							SNAPSHOT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS
						)) {
							throw new IllegalStateException(
								"Timed out waiting to complete first export");
						}
					}
					catch (InterruptedException ie) {
						interrupted.set(true);
						Thread.currentThread().interrupt();
					}
					firstExportCompleted.set(true);
				}
				else {
					finalExportFollowedFirst.set(firstExportCompleted.get());
				}
			}
			finally {
				exportInProgress.set(false);
			}
		}
	}
}
