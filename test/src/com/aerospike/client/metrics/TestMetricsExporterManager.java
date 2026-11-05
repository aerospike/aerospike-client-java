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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class TestMetricsExporterManager {
	@Test
	public void testExporterContractContainsOnlyExport() {
		assertEquals(1, IMetricsExporter.class.getDeclaredMethods().length);
		assertEquals("export", IMetricsExporter.class.getDeclaredMethods()[0].getName());
	}

	@Test
	public void testExportCallsAllExporters() {
		CountingExporter first = new CountingExporter();
		CountingExporter second = new CountingExporter();
		MetricsPolicy policy = policy(first, second);
		MetricsExporterManager manager = manager(policy, new AtomicLong());

		manager.export(snapshot());
		assertEquals(1, first.callCount.get());
		assertEquals(1, second.callCount.get());
	}

	@Test
	public void testExportUsesRegistrationOrder() {
		List<Integer> order = new ArrayList<>();
		MetricsPolicy policy = policy(
			snapshot -> order.add(1),
			snapshot -> order.add(2),
			snapshot -> order.add(3));

		manager(policy, new AtomicLong()).export(snapshot());
		assertEquals(List.of(1, 2, 3), order);
	}

	@Test
	public void testExporterRegistrationsAreFrozenAtConstruction() {
		CountingExporter initial = new CountingExporter();
		CountingExporter added = new CountingExporter();
		MetricsPolicy policy = policy(initial);
		MetricsExporterManager manager = manager(policy, new AtomicLong());

		policy.addExporter(added);
		manager.export(snapshot());

		assertEquals(1, initial.callCount.get());
		assertEquals(0, added.callCount.get());
	}

	@Test
	public void testExceptionDoesNotPreventHealthyExporter() {
		SequenceExporter failing = new SequenceExporter(true);
		CountingExporter healthy = new CountingExporter();
		MetricsPolicy policy = policy(failing, healthy);
		MetricsExporterManager manager = manager(policy, new AtomicLong());

		manager.export(snapshot());
		assertEquals(1, failing.callCount.get());
		assertEquals(1, healthy.callCount.get());
	}

	@Test
	public void testSuccessResetsConsecutiveFailures() {
		SequenceExporter exporter = new SequenceExporter(true, false, true, false);
		MetricsPolicy policy = policy(exporter);
		policy.maxConsecutiveFailures = 2;
		MetricsExporterManager manager = manager(policy, new AtomicLong());

		for (int i = 0; i < 4; i++) {
			manager.export(snapshot());
		}
		assertEquals(4, exporter.callCount.get());
	}

	@Test
	public void testExporterSuspendedAfterConsecutiveFailures() {
		SequenceExporter exporter = new SequenceExporter(true, true, true);
		MetricsPolicy policy = policy(exporter);
		policy.maxConsecutiveFailures = 2;
		policy.suspendRetryInterval = 60;
		AtomicLong nanoTime = new AtomicLong();
		MetricsExporterManager manager = manager(policy, nanoTime);

		manager.export(snapshot());
		manager.export(snapshot());
		manager.export(snapshot());
		assertEquals(2, exporter.callCount.get());
		assertFalse(manager.hasRunnableExporter());
	}

	@Test
	public void testSuspendedExporterRetriesAndResumes() {
		SequenceExporter exporter = new SequenceExporter(true, true, false, false);
		MetricsPolicy policy = policy(exporter);
		policy.maxConsecutiveFailures = 2;
		policy.suspendRetryInterval = 60;
		AtomicLong nanoTime = new AtomicLong();
		MetricsExporterManager manager = manager(policy, nanoTime);

		manager.export(snapshot());
		manager.export(snapshot());
		manager.export(snapshot());
		assertEquals(2, exporter.callCount.get());
		assertFalse(manager.hasRunnableExporter());

		nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(60) - 1);
		assertFalse(manager.hasRunnableExporter());

		nanoTime.incrementAndGet();
		assertTrue(manager.hasRunnableExporter());
		manager.export(snapshot());
		manager.export(snapshot());

		assertEquals(4, exporter.callCount.get());
	}

	private static MetricsPolicy policy(IMetricsExporter... exporters) {
		MetricsPolicy policy = new MetricsPolicy();

		for (IMetricsExporter exporter : exporters) {
			policy.addExporter(exporter);
		}
		return policy;
	}

	private static MetricsExporterManager manager(
		MetricsPolicy policy,
		AtomicLong nanoTime
	) {
		return new MetricsExporterManager(
			policy,
			policy.getExporters(),
			nanoTime::get
		);
	}

	private static MetricsSnapshot snapshot() {
		return new MetricsSnapshot(
			Instant.EPOCH,
			false,
			false,
			false,
			"cluster",
			"java",
			"1.0",
			"app",
			Collections.emptyMap(),
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0.0, 0,
			null, null, null, null,
			0, 0, 0,
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptyList(),
			new UsageMetricsSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
			LatencyUnit.MILLISECONDS,
			7,
			1
		);
	}

	private static class CountingExporter implements IMetricsExporter {
		final AtomicInteger callCount = new AtomicInteger();

		@Override
		public void export(MetricsSnapshot snapshot) {
			callCount.incrementAndGet();
		}
	}

	private static final class SequenceExporter extends CountingExporter {
		final boolean[] failures;

		SequenceExporter(boolean... failures) {
			this.failures = failures;
		}

		@Override
		public void export(MetricsSnapshot snapshot) {
			int call = callCount.getAndIncrement();

			if (call < failures.length && failures[call]) {
				throw new RuntimeException("expected failure");
			}
		}
	}
}
