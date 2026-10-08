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
package com.aerospike.client.cluster;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.policy.ClientPolicy;

public class SeedRefresherTest {
	private static final Host[] INIT = new Host[] {new Host("10.0.0.1", 3000)};
	private static final Host A = new Host("10.0.0.2", 3000);
	private static final Host B = new Host("10.0.0.3", 3000);

	private final List<String> warnings = new ArrayList<>();
	private final List<String> errors = new ArrayList<>();
	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private SeedRefresher refresher;

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			synchronized (warnings) {
				if (level == Log.Level.WARN) {
					warnings.add(message);
				}
				else if (level == Log.Level.ERROR) {
					errors.add(message);
				}
			}
		});
		snapshot.set(INIT);
	}

	@After
	public void tearDown() {
		if (refresher != null) {
			refresher.close();
		}
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private SeedRefresher create(TestSeedProvider provider, DiscoveryExecutionMode mode, int refreshInterval) {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryExecutionMode = mode;
		policy.discoveryRefreshInterval = refreshInterval;
		policy.discoveryTendRefreshDeadline = 20;
		policy.discoveryRefreshTimeout = 50;
		refresher = new SeedRefresher(provider, policy, snapshot::set);
		refresher.start(INIT);
		return refresher;
	}

	@Test
	public void tendModeCallsProviderOncePerThirtyTends() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, DiscoveryExecutionMode.TEND, 30000);

		for (int tendCount = 1; tendCount <= 89; tendCount++) {
			r.tend(tendCount, 1000);
			assertEquals("tendCount " + tendCount, tendCount / 30, provider.calls.get());
		}
		r.tend(90, 1000);
		assertEquals(3, provider.calls.get());
		assertArrayEquals(new Host[] {A}, snapshot.get());
		assertNull(r.getThread());
	}

	@Test
	public void tendModeProviderThrowableKeepsSnapshot() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, DiscoveryExecutionMode.TEND, 1000);

		r.tend(1, 1000);
		assertArrayEquals(new Host[] {A}, snapshot.get());
		Host[] lastGood = snapshot.get();

		Throwable[] throwables = new Throwable[] {
			new RuntimeException("runtime"), new Exception("checked"), new StackOverflowError(), null
		};

		for (Throwable t : throwables) {
			provider.error = t;
			provider.result = call -> (t == null)? null : List.of(B);
			r.tend(2, 1000);
			assertSame(lastGood, snapshot.get());
		}
		assertEquals(5, provider.calls.get());
		assertEquals(0, r.getOverrunCount());
		assertFalse(r.isTripped());
		assertEquals(4, warnings.size());

		provider.error = null;
		provider.result = call -> List.of(B);
		r.tend(3, 1000);
		assertArrayEquals(new Host[] {B}, snapshot.get());
	}

	@Test
	public void tendModeOverrunBlocksTendAndKeepsLastGood() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, DiscoveryExecutionMode.TEND, 1000);

		r.tend(1, 1000);
		Host[] lastGood = snapshot.get();
		assertArrayEquals(new Host[] {A}, lastGood);

		provider.sleepMillis = 100;
		provider.result = call -> List.of(B);

		long begin = System.nanoTime();
		r.tend(2, 1000);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertTrue("tend returned after " + elapsed + "ms", elapsed >= 100);
		assertSame(lastGood, snapshot.get());
		assertEquals(1, r.getOverrunCount());
		assertFalse(r.isTripped());
		assertEquals(1, warnings.size());

		r.tend(3, 1000);
		assertEquals(2, r.getOverrunCount());

		provider.sleepMillis = 0;
		r.tend(4, 1000);
		assertEquals(0, r.getOverrunCount());
		assertArrayEquals(new Host[] {B}, snapshot.get());
	}

	@Test
	public void tendModeThreeConsecutiveOverrunsTrip() {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, DiscoveryExecutionMode.TEND, 1000);

		r.tend(1, 1000);
		assertArrayEquals(new Host[] {A}, snapshot.get());

		provider.sleepMillis = 40;

		for (int i = 1; i <= SeedRefresher.MAX_OVERRUNS; i++) {
			r.tend(1 + i, 1000);
			assertEquals(i, r.getOverrunCount());
		}

		assertTrue(r.isTripped());
		assertSame(INIT, snapshot.get());
		assertEquals(1, errors.size());

		provider.sleepMillis = 0;
		int calls = provider.calls.get();

		for (int tendCount = 10; tendCount < 100; tendCount++) {
			r.tend(tendCount, 1000);
		}
		assertEquals(calls, provider.calls.get());
		assertSame(INIT, snapshot.get());

		SeedRefresher fresh = new SeedRefresher(provider, policy(DiscoveryExecutionMode.TEND), snapshot::set);
		fresh.start(INIT);
		assertFalse(fresh.isTripped());
		assertEquals(0, fresh.getOverrunCount());
		fresh.tend(30, 1000);
		assertEquals(calls + 1, provider.calls.get());
		assertArrayEquals(new Host[] {A}, snapshot.get());
	}

	@Test
	public void threadModeNeverBlocksTend() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		CountDownLatch block = new CountDownLatch(1);
		provider.block = block;
		SeedRefresher r = create(provider, DiscoveryExecutionMode.THREAD, 1);

		awaitCalls(provider, 1);

		long begin = System.nanoTime();

		for (int tendCount = 1; tendCount <= 1000; tendCount++) {
			r.tend(tendCount, 1);
			assertSame(INIT, snapshot.get());
		}
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		assertTrue("tend took " + elapsed + "ms", elapsed < 50);
		assertEquals(1, provider.calls.get());

		provider.block = null;
		block.countDown();
		awaitSnapshot(new Host[] {A});
	}

	@Test
	public void threadModeTimeoutDiscardsResult() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		provider.sleepMillis = 100;
		SeedRefresher r = create(provider, DiscoveryExecutionMode.THREAD, 1);

		awaitCalls(provider, 3);
		assertSame(INIT, snapshot.get());
		assertTrue(warnings.size() >= 2);

		provider.sleepMillis = 0;
		awaitSnapshot(new Host[] {A});
		assertEquals(0, r.getOverrunCount());
		assertFalse(r.isTripped());
	}

	@Test
	public void threadModeThrowableKeepsLastGood() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		create(provider, DiscoveryExecutionMode.THREAD, 1);

		awaitSnapshot(new Host[] {A});
		Host[] lastGood = snapshot.get();

		provider.error = new Error("provider failed");
		provider.result = call -> List.of(B);
		int calls = provider.calls.get();
		awaitCalls(provider, calls + 3);
		assertSame(lastGood, snapshot.get());
	}

	@Test
	public void threadModeStoppedByClose() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, A);
		SeedRefresher r = create(provider, DiscoveryExecutionMode.THREAD, 60000);

		Thread thread = r.getThread();
		assertTrue(thread.isAlive());
		assertTrue(thread.isDaemon());

		r.close();
		thread.join(5000);
		assertFalse(thread.isAlive());
		assertEquals(0, provider.calls.get());
	}

	@Test
	public void noPeriodicRefreshDoesNoWork() {
		for (DiscoveryExecutionMode mode : DiscoveryExecutionMode.values()) {
			TestSeedProvider provider = new TestSeedProvider(false, true, A);
			SeedRefresher r = create(provider, mode, 1000);

			assertNull(r.getThread());

			for (int tendCount = 1; tendCount <= 100; tendCount++) {
				r.tend(tendCount, 1000);
			}
			assertEquals(0, provider.calls.get());
			assertSame(INIT, snapshot.get());
			r.close();
		}
	}

	private static ClientPolicy policy(DiscoveryExecutionMode mode) {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryExecutionMode = mode;
		return policy;
	}

	private static void awaitCalls(TestSeedProvider provider, int calls) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (provider.calls.get() < calls) {
			assertTrue("provider calls " + provider.calls.get() + " < " + calls, System.nanoTime() < deadline);
			Thread.sleep(1);
		}
	}

	private void awaitSnapshot(Host[] expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! Arrays.equals(expected, snapshot.get())) {
			assertTrue("snapshot not published", System.nanoTime() < deadline);
			Thread.sleep(1);
		}
	}
}
