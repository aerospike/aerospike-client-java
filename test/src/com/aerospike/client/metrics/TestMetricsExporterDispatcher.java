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
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class TestMetricsExporterDispatcher {

	@Test
	public void testDispatchCallsAllExporters() {
		CountingExporter first = new CountingExporter();
		CountingExporter second = new CountingExporter();
		MetricsPolicy policy = policy(first, second);
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());

		try {
			dispatcher.dispatch(snapshot());
			assertEquals(1, first.callCount.get());
			assertEquals(1, second.callCount.get());
		}
		finally {
			dispatcher.shutdown();
		}
	}

	@Test
	public void testExporterRegistrationsAreFrozenAtConstruction() {
		CountingExporter initial = new CountingExporter();
		CountingExporter added = new CountingExporter();
		MetricsPolicy policy = policy(initial);
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());

		try {
			policy.addExporter(added);
			dispatcher.dispatch(snapshot());

			assertEquals(1, initial.callCount.get());
			assertEquals(0, added.callCount.get());
		}
		finally {
			dispatcher.shutdown();
		}
	}

	@Test
	public void testExceptionDoesNotPreventHealthyExporter() {
		SequenceExporter failing = new SequenceExporter(true);
		CountingExporter healthy = new CountingExporter();
		MetricsPolicy policy = policy(failing, healthy);
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());

		try {
			dispatcher.dispatch(snapshot());
			assertEquals(1, failing.callCount.get());
			assertEquals(1, healthy.callCount.get());
		}
		finally {
			dispatcher.shutdown();
		}
	}

	@Test
	public void testSuccessResetsConsecutiveFailures() {
		SequenceExporter exporter = new SequenceExporter(true, false, true, false);
		MetricsPolicy policy = policy(exporter);
		policy.maxConsecutiveFailures = 2;
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());

		try {
			for (int i = 0; i < 4; i++) {
				dispatcher.dispatch(snapshot());
			}
			assertEquals(4, exporter.callCount.get());
		}
		finally {
			dispatcher.shutdown();
		}
	}

	@Test
	public void testExporterSuspendedAfterConsecutiveFailures() {
		SequenceExporter exporter = new SequenceExporter(true, true, true);
		MetricsPolicy policy = policy(exporter);
		policy.maxConsecutiveFailures = 2;
		policy.suspendRetryInterval = 60;
		AtomicLong clock = new AtomicLong();
		MetricsExporterDispatcher dispatcher = dispatcher(policy, clock);

		try {
			dispatcher.dispatch(snapshot());
			dispatcher.dispatch(snapshot());
			dispatcher.dispatch(snapshot());
			assertEquals(2, exporter.callCount.get());
		}
		finally {
			dispatcher.shutdown();
		}
	}

	@Test
	public void testSuspendedExporterRetriesAndResumes() {
		SequenceExporter exporter = new SequenceExporter(true, true, false, false);
		MetricsPolicy policy = policy(exporter);
		policy.maxConsecutiveFailures = 2;
		policy.suspendRetryInterval = 60;
		AtomicLong clock = new AtomicLong();
		MetricsExporterDispatcher dispatcher = dispatcher(policy, clock);

		try {
			dispatcher.dispatch(snapshot());
			dispatcher.dispatch(snapshot());
			dispatcher.dispatch(snapshot());
			assertEquals(2, exporter.callCount.get());

			clock.addAndGet(TimeUnit.SECONDS.toMillis(60));
			dispatcher.dispatch(snapshot());
			dispatcher.dispatch(snapshot());

			assertEquals(4, exporter.callCount.get());
		}
		finally {
			dispatcher.shutdown();
		}
	}

	@Test
	public void testTimeoutDoesNotBlockHealthyExporter() throws Exception {
		UninterruptibleExporter stuck = new UninterruptibleExporter();
		CountingExporter healthy = new CountingExporter();
		MetricsPolicy policy = policy(stuck, healthy);
		policy.exportTimeout = 10;
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());

		try {
			dispatcher.dispatch(snapshot());
			assertEquals(1, stuck.callCount.get());
			assertTrue("Stuck exporter should observe cancellation",
				stuck.interruptedLatch.await(1, TimeUnit.SECONDS));
			assertEquals(1, healthy.callCount.get());
		}
		finally {
			stuck.release();
			dispatcher.shutdown();
		}
	}

	@Test
	public void testShutdownIsIdempotentAndStopsDispatch() {
		CountingExporter exporter = new CountingExporter();
		MetricsPolicy policy = policy(exporter);
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());

		dispatcher.shutdown();
		dispatcher.shutdown();
		dispatcher.dispatch(snapshot());

		assertEquals(0, exporter.callCount.get());
	}

	@Test
	public void testShutdownCancelsInFlightDispatch() throws Exception {
		UninterruptibleExporter stuck = new UninterruptibleExporter();
		MetricsPolicy policy = policy(stuck);
		policy.exportTimeout = 10000;
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());
		Thread caller = new Thread(() -> dispatcher.dispatch(snapshot()));

		try {
			caller.start();
			assertTrue("Exporter should start", stuck.startedLatch.await(1, TimeUnit.SECONDS));

			dispatcher.shutdown();
			caller.join(1000);

			assertFalse("Dispatch caller should stop after shutdown", caller.isAlive());
		}
		finally {
			stuck.release();
			dispatcher.shutdown();
		}
	}

	@Test
	public void testInterruptedDispatchStopsBeforeNextExporter() throws Exception {
		UninterruptibleExporter stuck = new UninterruptibleExporter();
		CountingExporter next = new CountingExporter();
		MetricsPolicy policy = policy(stuck, next);
		policy.exportTimeout = 10000;
		MetricsExporterDispatcher dispatcher = dispatcher(policy, new AtomicLong());
		Thread caller = new Thread(() -> dispatcher.dispatch(snapshot()));

		try {
			caller.start();
			assertTrue("Exporter should start", stuck.startedLatch.await(1, TimeUnit.SECONDS));

			caller.interrupt();
			caller.join(1000);

			assertFalse("Interrupted dispatch should stop", caller.isAlive());
			assertTrue("Interrupt status should be preserved", caller.isInterrupted());
			assertEquals("No later exporter should run", 0, next.callCount.get());
		}
		finally {
			stuck.release();
			dispatcher.shutdown();
		}
	}

	private static MetricsPolicy policy(IMetricsExporter... exporters) {
		MetricsPolicy policy = new MetricsPolicy();

		for (IMetricsExporter exporter : exporters) {
			policy.addExporter(exporter);
		}
		return policy;
	}

	private static MetricsExporterDispatcher dispatcher(
		MetricsPolicy policy,
		AtomicLong clock
	) {
		return new MetricsExporterDispatcher(
			policy,
			clock::get,
			TestMetricsExporterDispatcher::newExecutor,
			TimeUnit.MILLISECONDS
		);
	}

	private static ExecutorService newExecutor() {
		return Executors.newSingleThreadExecutor(runnable -> {
			Thread thread = new Thread(runnable, "metrics-dispatch-test");
			thread.setDaemon(true);
			return thread;
		});
	}

	private static MetricsSnapshot snapshot() {
		return new MetricsSnapshot(
			Instant.EPOCH,
			false,
			"cluster",
			"java",
			"1.0",
			"app",
			Collections.emptyMap(),
			0, 0, 0, 0, 0, 0, 0, 0,
			0.0, 0, 0,
			Collections.emptyList(),
			Collections.emptyList(),
			null,
			MetricsSnapshot.HistogramType.LOGARITHMIC,
			MetricsSnapshot.LatencyUnit.MILLISECONDS,
			2,
			7
		);
	}

	private static class CountingExporter implements IMetricsExporter {
		final AtomicInteger callCount = new AtomicInteger();

		@Override
		public void export(MetricsSnapshot snapshot) {
			callCount.incrementAndGet();
		}

		@Override
		public void close() {
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

	private static final class UninterruptibleExporter extends CountingExporter {
		final CountDownLatch startedLatch = new CountDownLatch(1);
		final CountDownLatch interruptedLatch = new CountDownLatch(1);
		final CountDownLatch releaseLatch = new CountDownLatch(1);

		@Override
		public void export(MetricsSnapshot snapshot) {
			callCount.incrementAndGet();
			startedLatch.countDown();
			boolean released = false;

			while (!released) {
				try {
					releaseLatch.await();
					released = true;
				}
				catch (InterruptedException e) {
					interruptedLatch.countDown();
				}
			}
		}

		void release() {
			releaseLatch.countDown();
		}
	}
}
