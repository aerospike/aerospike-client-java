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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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

public class TestMetricsSnapshot {

	@Test
	public void testSnapshotCollectionsAreUnmodifiable() {
		MetricsSnapshot snapshot = buildMinimalSnapshot();
		NodeSnapshot node = snapshot.nodes.get(0);
		HistogramSnapshot histogram =
			new HistogramSnapshot(new long[]{0}, 0, 0, 0.0, 0);
		NamespaceSnapshot namespace = new NamespaceSnapshot(
			"test", 0, 0, 0, 0, 0,
			Collections.emptyMap(), Collections.emptyMap()
		);
		CommandSnapshot command = new CommandSnapshot(
			CommandType.GET,
			histogram, histogram, histogram, histogram, histogram, histogram,
			0, 0, Collections.emptyMap()
		);

		assertThrows(UnsupportedOperationException.class,
			() -> snapshot.labels.put("env", "test"));
		assertThrows(UnsupportedOperationException.class,
			() -> snapshot.eventLoops.add(new EventLoopSnapshot(0, 0)));
		assertThrows(UnsupportedOperationException.class,
			() -> snapshot.nodes.add(buildMinimalNodeSnapshot()));
		assertThrows(UnsupportedOperationException.class,
			() -> node.namespaces.add(namespace));
		assertThrows(UnsupportedOperationException.class,
			() -> node.commandLatencies.put(CommandType.GET, histogram));
		assertThrows(UnsupportedOperationException.class,
			() -> namespace.compatibilityLatencies.put(LatencyType.READ, histogram));
		assertThrows(UnsupportedOperationException.class,
			() -> command.resultCodeCounts.put(0, 1L));
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

	@Test
	public void testSnapshotDefensivelyCopiesSourceCollections() {
		Map<String, String> labels = new HashMap<>();
		labels.put("env", "test");
		List<EventLoopSnapshot> eventLoops = new ArrayList<>();
		eventLoops.add(new EventLoopSnapshot(1, 2));
		List<NodeSnapshot> nodes = new ArrayList<>();
		nodes.add(buildMinimalNodeSnapshot());

		MetricsSnapshot snapshot = new MetricsSnapshot(
			Instant.now(), true, "cluster", "java", "1.0", "app",
			labels,
			0, 0, 0, 0, 1, 1, 0, 0,
			0.0, 0, 0,
			eventLoops,
			nodes, null,
			HistogramType.LOGARITHMIC, LatencyUnit.MILLISECONDS, 2, 7
		);

		labels.put("injected", "value");
		eventLoops.clear();
		nodes.clear();

		assertFalse(snapshot.labels.containsKey("injected"));
		assertEquals(1, snapshot.eventLoops.size());
		assertEquals(1, snapshot.nodes.size());
	}

	@Test
	public void testNodeSnapshotDefensivelyCopiesSourceCollections() {
		Map<CommandType, HistogramSnapshot> latencies = new HashMap<>();
		List<NamespaceSnapshot> namespaces = new ArrayList<>();
		NodeSnapshot snapshot = buildNodeSnapshot(latencies, namespaces);

		latencies.put(CommandType.GET,
			new HistogramSnapshot(new long[]{1}, 1, 1, 1.0, 1));
		namespaces.add(new NamespaceSnapshot(
			"test", 0, 0, 0, 0, 0,
			Collections.emptyMap(), Collections.emptyMap()
		));

		assertTrue(snapshot.commandLatencies.isEmpty());
		assertTrue(snapshot.namespaces.isEmpty());
	}

	private static MetricsSnapshot buildMinimalSnapshot() {
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

	private static NodeSnapshot buildMinimalNodeSnapshot() {
		Map<CommandType, HistogramSnapshot> emptyLatencies = new HashMap<>();
		List<NamespaceSnapshot> emptyNamespaces = new ArrayList<>();
		return buildNodeSnapshot(emptyLatencies, emptyNamespaces);
	}

	private static NodeSnapshot buildNodeSnapshot(
		Map<CommandType, HistogramSnapshot> commandLatencies,
		List<NamespaceSnapshot> namespaces
	) {
		ConnectionSnapshot syncConns = new ConnectionSnapshot(1, 5, 10, 2);
		ConnectionSnapshot asyncConns = new ConnectionSnapshot(0, 0, 0, 0);

		return new NodeSnapshot(
			"node1", "127.0.0.1", 3000,
			syncConns, asyncConns,
			0, 0, 0, 0, 0, 0, 0, 0, 0,
			6, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			commandLatencies, namespaces
		);
	}
}
