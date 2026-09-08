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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.metrics.MetricsSnapshot;
import com.aerospike.client.metrics.MetricsSnapshot.CommandSnapshot;
import com.aerospike.client.metrics.MetricsSnapshot.CommandType;
import com.aerospike.client.metrics.MetricsSnapshot.ConnectionSnapshot;
import com.aerospike.client.metrics.MetricsSnapshot.EventLoopSnapshot;
import com.aerospike.client.metrics.MetricsSnapshot.HistogramSnapshot;
import com.aerospike.client.metrics.MetricsSnapshot.HistogramType;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.metrics.MetricsSnapshot.LatencyUnit;
import com.aerospike.client.metrics.MetricsSnapshot.NamespaceSnapshot;
import com.aerospike.client.metrics.MetricsSnapshot.NodeSnapshot;
import com.aerospike.test.sync.TestSync;

public class TestMetricsSnapshot extends TestSync {

	@Test(expected = UnsupportedOperationException.class)
	public void testSnapshotImmutability() {
		MetricsSnapshot snapshot = buildMinimalSnapshot();

		snapshot.nodes.add(buildMinimalNodeSnapshot());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNodeSnapshotNamespacesImmutability() {
		NodeSnapshot nodeSnapshot = buildMinimalNodeSnapshot();

		nodeSnapshot.namespaces.add(
			new NamespaceSnapshot("test", 0, 0, 0, 0, 0,
				Collections.emptyMap(), Collections.emptyMap())
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNodeSnapshotCommandLatenciesImmutability() {
		NodeSnapshot nodeSnapshot = buildMinimalNodeSnapshot();

		nodeSnapshot.commandLatencies.put(CommandType.GET,
			new HistogramSnapshot(new long[]{0}, 0, 0, 0.0, 0));
	}

	@Test
	public void testConnectionSnapshotValues() {
		ConnectionSnapshot conn = new ConnectionSnapshot(5, 10, 100, 50);

		assertEquals(5, conn.inUse);
		assertEquals(10, conn.inPool);
		assertEquals(100, conn.opened);
		assertEquals(50, conn.closed);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testLabelsImmutability() {
		Map<String, String> labels = new HashMap<>();
		labels.put("env", "test");

		List<NodeSnapshot> nodes = new ArrayList<>();
		nodes.add(buildMinimalNodeSnapshot());

		MetricsSnapshot snapshot = new MetricsSnapshot(
			Instant.now(), true, "cluster", "java", "1.0", "app",
			labels,
			0, 0, 0, 0, 1, 1, 0, 0,
			0.0, 0, 0,
			Collections.<EventLoopSnapshot>emptyList(),
			nodes, null,
			HistogramType.LOGARITHMIC, LatencyUnit.MILLISECONDS, 2, 7
		);

		snapshot.labels.put("injected", "value");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testEventLoopsImmutability() {
		List<EventLoopSnapshot> eventLoops = new ArrayList<>();
		eventLoops.add(new EventLoopSnapshot(5, 10));

		List<NodeSnapshot> nodes = new ArrayList<>();
		nodes.add(buildMinimalNodeSnapshot());

		MetricsSnapshot snapshot = new MetricsSnapshot(
			Instant.now(), true, "cluster", "java", "1.0", "app",
			Collections.emptyMap(),
			0, 0, 0, 0, 1, 1, 0, 0,
			0.0, 0, 0,
			eventLoops,
			nodes, null,
			HistogramType.LOGARITHMIC, LatencyUnit.MILLISECONDS, 2, 7
		);

		snapshot.eventLoops.add(new EventLoopSnapshot(0, 0));
	}

	@Test
	public void testHistogramSnapshotDefensiveCopy() {
		long[] originalBuckets = {10, 20, 30, 40};
		HistogramSnapshot histogram = new HistogramSnapshot(originalBuckets, 1, 100, 500.0, 4);

		long[] returnedBuckets = histogram.getBuckets();
		assertArrayEquals(originalBuckets, returnedBuckets);

		returnedBuckets[0] = 999;
		long[] freshBuckets = histogram.getBuckets();
		assertEquals(10, freshBuckets[0]);

		originalBuckets[0] = 888;
		long[] freshBuckets2 = histogram.getBuckets();
		assertEquals(10, freshBuckets2[0]);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNamespaceSnapshotCompatibilityLatenciesImmutability() {
		Map<LatencyType, HistogramSnapshot> latencies = new HashMap<>();
		latencies.put(LatencyType.READ, new HistogramSnapshot(new long[]{1, 2}, 0, 10, 5.0, 3));

		NamespaceSnapshot nsSnapshot = new NamespaceSnapshot(
			"test-ns", 0, 0, 0, 0, 0,
			latencies, Collections.emptyMap()
		);

		nsSnapshot.compatibilityLatencies.put(LatencyType.WRITE,
			new HistogramSnapshot(new long[]{0}, 0, 0, 0.0, 0));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testCommandSnapshotResultCodeCountsImmutability() {
		Map<Integer, Long> resultCodes = new HashMap<>();
		resultCodes.put(0, 100L);

		HistogramSnapshot emptyHist = new HistogramSnapshot(new long[]{0}, 0, 0, 0.0, 0);

		CommandSnapshot cmdSnapshot = new CommandSnapshot(
			CommandType.GET,
			emptyHist, emptyHist, emptyHist, emptyHist,
			emptyHist, emptyHist,
			0, 0, resultCodes
		);

		cmdSnapshot.resultCodeCounts.put(1, 50L);
	}

	/**
	 * Build a minimal MetricsSnapshot with one node and no namespaces.
	 */
	private MetricsSnapshot buildMinimalSnapshot() {
		List<NodeSnapshot> nodes = new ArrayList<>();
		nodes.add(buildMinimalNodeSnapshot());

		return new MetricsSnapshot(
			Instant.now(),
			true,
			"test-cluster",
			"java",
			"1.0.0",
			"test-app",
			Collections.emptyMap(),
			0, 0, 0, 0, 1, 1, 0, 0,
			0.0, 0, 0,
			Collections.<EventLoopSnapshot>emptyList(),
			nodes,
			null,
			HistogramType.LOGARITHMIC,
			LatencyUnit.MILLISECONDS,
			2, 7
		);
	}

	/**
	 * Build a minimal NodeSnapshot with empty namespaces and command latencies.
	 */
	private NodeSnapshot buildMinimalNodeSnapshot() {
		ConnectionSnapshot syncConns = new ConnectionSnapshot(1, 5, 10, 2);
		ConnectionSnapshot asyncConns = new ConnectionSnapshot(0, 0, 0, 0);
		Map<CommandType, HistogramSnapshot> emptyLatencies = new HashMap<>();
		List<NamespaceSnapshot> emptyNamespaces = new ArrayList<>();

		return new NodeSnapshot(
			"node1", "127.0.0.1", 3000,
			syncConns, asyncConns,
			0, 0, 0, 0, 0, 0, 0, 0, 0,
			6, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			emptyLatencies, emptyNamespaces
		);
	}
}
