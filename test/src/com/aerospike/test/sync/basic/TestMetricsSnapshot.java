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

import com.aerospike.client.metrics.ConnectionMetricsSnapshot;
import com.aerospike.client.metrics.EventLoopMetricsSnapshot;
import com.aerospike.client.metrics.LatencySnapshot;
import com.aerospike.client.metrics.LatencyUnit;
import com.aerospike.client.metrics.MetricsSnapshot;
import com.aerospike.client.metrics.NamespaceMetricsSnapshot;
import com.aerospike.client.metrics.NodeMetricsSnapshot;
import com.aerospike.client.metrics.UsageMetricsSnapshot;
import com.aerospike.client.metrics.LatencyType;

public class TestMetricsSnapshot {

	@Test
	public void testSnapshotCollectionsAreUnmodifiable() {
		MetricsSnapshot snapshot = buildMinimalSnapshot();
		NodeMetricsSnapshot node = snapshot.nodes.get(0);
		NamespaceMetricsSnapshot namespace = namespace();

		assertThrows(UnsupportedOperationException.class,
			() -> snapshot.labels.put("env", "test"));
		assertThrows(UnsupportedOperationException.class,
			() -> snapshot.eventLoops.add(new EventLoopMetricsSnapshot(0, 0)));
		assertThrows(UnsupportedOperationException.class,
			() -> snapshot.nodes.add(buildMinimalNodeSnapshot()));
		assertThrows(UnsupportedOperationException.class,
			() -> node.namespaces.add(namespace));
	}

	@Test
	public void testHistogramSnapshotDefensiveCopy() {
		long[] originalBuckets = {10, 20, 30, 40};
		LatencySnapshot histogram = new LatencySnapshot(
			LatencyType.READ, originalBuckets, 1L, 100L, 500.0, 4);

		long[] returnedBuckets = histogram.getBucketCounts();
		assertArrayEquals(originalBuckets, returnedBuckets);

		returnedBuckets[0] = 999;
		long[] freshBuckets = histogram.getBucketCounts();
		assertEquals(10, freshBuckets[0]);

		originalBuckets[0] = 888;
		long[] freshBuckets2 = histogram.getBucketCounts();
		assertEquals(10, freshBuckets2[0]);
	}

	@Test
	public void testSnapshotDefensivelyCopiesSourceCollections() {
		Map<String, String> labels = new HashMap<>();
		labels.put("env", "test");
		List<EventLoopMetricsSnapshot> eventLoops = new ArrayList<>();
		eventLoops.add(new EventLoopMetricsSnapshot(1, 2));
		List<NodeMetricsSnapshot> nodes = new ArrayList<>();
		nodes.add(buildMinimalNodeSnapshot());

		MetricsSnapshot snapshot = new MetricsSnapshot(
			Instant.now(), true, true, false, "cluster", "java", "1.0", "app",
			labels,
			1, 0, 0, 0, 1, 0, 0, 0, 0, 0,
			0.0, 0,
			null, null, null, null,
			0, 0, 0,
			eventLoops,
			nodes, Collections.emptyList(), usage(),
			LatencyUnit.MILLISECONDS, 7, 1
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
		List<NamespaceMetricsSnapshot> namespaces = new ArrayList<>();
		NodeMetricsSnapshot snapshot = buildNodeSnapshot(namespaces);
		namespaces.add(namespace());
		assertTrue(snapshot.namespaces.isEmpty());
	}

	@Test
	public void testSnapshotToStringIncludesFieldsAndHistogramValues() {
		LatencySnapshot latency = new LatencySnapshot(
			LatencyType.READ, new long[] {10, 20, 30}, null, null, null, 60);
		NamespaceMetricsSnapshot namespace = new NamespaceMetricsSnapshot(
			"test", 1, 2, 3, 4, 5, 6, 7, 8,
			latency, null, null, null);
		NodeMetricsSnapshot node = new NodeMetricsSnapshot(
			"node1", "127.0.0.1", 3000,
			new ConnectionMetricsSnapshot(1, 2, 3, 4),
			new ConnectionMetricsSnapshot(5, 6, 7, 8),
			14, latency, 9, 10, 11, Collections.singletonList(namespace));
		UsageMetricsSnapshot usage = new UsageMetricsSnapshot(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		MetricsSnapshot snapshot = new MetricsSnapshot(
			Instant.EPOCH, true, true, true, "cluster", "java", "1.0", "app",
			Collections.singletonMap("env", "test"),
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11.5, 12,
			latency, null, null, null,
			13, 14, 15,
			Collections.singletonList(new EventLoopMetricsSnapshot(16, 17)),
			Collections.singletonList(node), Collections.singletonList(node), usage,
			LatencyUnit.MILLISECONDS, 7, 1);

		String value = snapshot.toString();
		assertTrue(value.contains("MetricsSnapshot{timestamp=1970-01-01T00:00:00Z"));
		assertTrue(value.contains("exceededMaxRetries=9"));
		assertTrue(value.contains("validationAuthFailure=15"));
		assertTrue(value.contains("EventLoopMetricsSnapshot{processSize=16, queueSize=17}"));
		assertTrue(value.contains("ConnectionMetricsSnapshot{inUse=1, inPool=2, opened=3, closed=4}"));
		assertTrue(value.contains("NamespaceMetricsSnapshot{namespace='test'"));
		assertTrue(value.contains("UsageMetricsSnapshot{blocking=1"));
		assertTrue(value.contains("bucketCounts=[10, 20, 30]"));
		assertFalse(value.contains("[J@"));
	}

	private static MetricsSnapshot buildMinimalSnapshot() {
		List<NodeMetricsSnapshot> nodes = new ArrayList<>();
		nodes.add(buildMinimalNodeSnapshot());

		return new MetricsSnapshot(
			Instant.now(),
			true,
			true,
			false,
			"test-cluster",
			"java",
			"1.0.0",
			"test-app",
			Collections.emptyMap(),
			1, 0, 0, 0, 1, 0, 0, 0, 0, 0,
			0.0, 0,
			null, null, null, null,
			0, 0, 0,
			Collections.<EventLoopMetricsSnapshot>emptyList(),
			nodes,
			Collections.emptyList(),
			usage(),
			LatencyUnit.MILLISECONDS,
			7, 1
		);
	}

	private static NodeMetricsSnapshot buildMinimalNodeSnapshot() {
		return buildNodeSnapshot(new ArrayList<>());
	}

	private static NodeMetricsSnapshot buildNodeSnapshot(
		List<NamespaceMetricsSnapshot> namespaces
	) {
		ConnectionMetricsSnapshot syncConns = new ConnectionMetricsSnapshot(1, 5, 10, 2);
		ConnectionMetricsSnapshot asyncConns = new ConnectionMetricsSnapshot(0, 0, 0, 0);

		return new NodeMetricsSnapshot(
			"node1", "127.0.0.1", 3000,
			syncConns, asyncConns,
			6, null, 0, 0, 0, namespaces
		);
	}

	private static NamespaceMetricsSnapshot namespace() {
		return new NamespaceMetricsSnapshot(
			"test", 0, 0, 0, 0, 0, 0, 0, 0,
			null, null, null, null);
	}

	private static UsageMetricsSnapshot usage() {
		return new UsageMetricsSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
	}
}
