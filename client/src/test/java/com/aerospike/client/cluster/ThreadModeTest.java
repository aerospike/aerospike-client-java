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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.discovery.StaticSeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;

public class ThreadModeTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host S = new Host("10.0.1.1", 3000);
	private static final Host A = new Host("10.0.0.1", 3000);
	private static final Host B = new Host("10.0.0.2", 3000);
	private static final String OVERRUN = "Seed candidate refresh took ";
	private static final String FAILED = "Seed candidate refresh failed: ";
	private static final String SEED_PREFIX = "Seed 127.0.0.1 ";
	private static final int BOUND = 150;

	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private final List<String> events = new CopyOnWriteArrayList<>();
	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private final List<Provider> providers = new ArrayList<>();

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN && message.startsWith(SEED_PREFIX)) {
				events.add("seed " + message.substring(SEED_PREFIX.length(), message.indexOf(' ', SEED_PREFIX.length())));
			}
			else if (level == Log.Level.WARN || level == Log.Level.ERROR) {
				warnings.add(message);
			}
		});
	}

	@After
	public void tearDown() {
		for (Provider provider : providers) {
			provider.release.countDown();
		}
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	@FunctionalInterface
	private interface Script {
		List<Host> call(int call, Provider provider) throws Exception;
	}

	private final class Provider implements SeedCandidateProvider {
		final AtomicInteger calls = new AtomicInteger();
		final AtomicInteger returned = new AtomicInteger();
		final AtomicInteger closes = new AtomicInteger();
		final AtomicLong tendNanos = new AtomicLong();
		final List<String> threads = new CopyOnWriteArrayList<>();
		final CountDownLatch release = new CountDownLatch(1);
		volatile String closeThread;
		private final boolean periodic;
		private final Script script;

		Provider(boolean periodic, Script script) {
			this.periodic = periodic;
			this.script = script;
			providers.add(this);
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			int call = calls.incrementAndGet();
			String name = Thread.currentThread().getName();
			threads.add(name);
			events.add("refresh " + call);
			long begin = System.nanoTime();

			try {
				return script.call(call, this);
			}
			catch (RuntimeException e) {
				throw e;
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
			finally {
				if (name.equals("tend")) {
					tendNanos.addAndGet(System.nanoTime() - begin);
				}
				returned.incrementAndGet();
			}
		}

		List<Host> sleep(long millis, Host... hosts) throws InterruptedException {
			Thread.sleep(millis);
			return Arrays.asList(hosts);
		}

		List<Host> block(Host... hosts) throws InterruptedException {
			release.await(60, TimeUnit.SECONDS);
			return Arrays.asList(hosts);
		}

		@Override
		public boolean needsPeriodicRefresh() {
			return periodic;
		}

		@Override
		public boolean supportsDeadline() {
			return true;
		}

		@Override
		public void close() {
			closeThread = Thread.currentThread().getName();
			closes.incrementAndGet();
		}
	}

	private static ClientPolicy policy(SeedCandidateProvider provider) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.discoveryRefreshInterval = 60000;
		policy.seedCandidateProvider = provider;
		return policy;
	}

	private SeedRefresher refresher(Provider provider, Host[] staticSeeds, int interval, int timeout) {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryRefreshInterval = interval;
		policy.discoveryRefreshTimeout = timeout;
		SeedRefresher r = new SeedRefresher(provider, policy, staticSeeds, snapshot::set);
		r.validate(interval);
		return r;
	}

	private Host[] start(SeedRefresher r) {
		Host[] seeds = r.discover();
		snapshot.set(seeds);
		r.start(seeds);
		return seeds;
	}

	private long count(String prefix) {
		return warnings.stream().filter(w -> w.startsWith(prefix)).count();
	}

	@Test
	public void policyDefaultsAndCopy() {
		ClientPolicy policy = new ClientPolicy();
		assertEquals(DiscoveryExecutionMode.THREAD, policy.discoveryExecutionMode);
		assertEquals(2000, policy.discoveryRefreshTimeout);

		policy.setDiscoveryExecutionMode(DiscoveryExecutionMode.TEND);
		policy.setDiscoveryRefreshTimeout(700);
		ClientPolicy copy = new ClientPolicy(policy);
		assertEquals(DiscoveryExecutionMode.TEND, copy.discoveryExecutionMode);
		assertEquals(700, copy.discoveryRefreshTimeout);
	}

	@Test
	public void defaultPluginsStartNoDiscoveryThread() throws Exception {
		SeedCandidateProvider[] list = new SeedCandidateProvider[] {
			null, new StaticSeedCandidateProvider(DEAD_SEED), new Provider(false, (call, p) -> Arrays.asList(DEAD_SEED))
		};

		for (SeedCandidateProvider provider : list) {
			ClientPolicy policy = policy(provider);
			Set<Thread> before = Thread.getAllStackTraces().keySet();
			AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

			try {
				Cluster cluster = client.getCluster();
				assertNull(cluster.seedRefresher.getThread());
				assertEquals(Arrays.asList("tend"), newThreadNames(before));
			}
			finally {
				client.close();
			}
		}
	}

	@Test
	public void periodicProviderRunsOnDiscoveryThread() throws Exception {
		Provider provider = new Provider(true, (call, p) -> Arrays.asList(new Host("127.0.0.1", 10 + call)));
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshInterval = 250;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			assertEquals(Arrays.asList("discovery", "tend"), newThreadNames(before));
			assertTrue(cluster.seedRefresher.getThread().isDaemon());
			await(() -> provider.calls.get() >= 4);
			await(() -> cluster.getSeeds().length == 2 && cluster.getSeeds()[1].port >= 13);
		}
		finally {
			client.close();
		}
		assertEquals(Collections.singleton("discovery"), new HashSet<>(provider.threads));
	}

	@Test
	public void blockingProviderNeverBlocksTend() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? Arrays.asList(DEAD_SEED) : p.block(B));
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshInterval = 250;

		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			await(() -> provider.calls.get() == 2);
			int tends = cluster.getInvalidNodeCount();
			long begin = System.nanoTime();
			await(() -> cluster.getInvalidNodeCount() >= tends + 8);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			System.out.println("THREAD-MODE-BLOCKED tends=" + (cluster.getInvalidNodeCount() - tends) + " in " + elapsed +
				"ms while the provider blocks; tend time in provider=" + provider.tendNanos.get() + "ns");

			assertEquals(2, provider.calls.get());
			assertEquals(1, provider.returned.get());
			assertEquals(0, provider.tendNanos.get());
			assertFalse(provider.threads.contains("tend"));
			assertTrue(elapsed + "ms", elapsed < 8 * 250 + 1000);
		}
		finally {
			client.close();
		}
	}

	@Test
	public void lateSuccessIsPublished() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? Arrays.asList(A) : (call == 2)? p.sleep(150, B) : p.block());
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			assertArrayEquals(new Host[] {A}, start(r));
			await(() -> provider.calls.get() == 3);

			assertArrayEquals(new Host[] {B}, snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(warnings.toString(), 1, count(OVERRUN));
			assertTrue(warnings.toString(), warnings.stream().anyMatch(w ->
				w.startsWith(OVERRUN) && w.contains("exceeding refresh timeout 100ms")));
		}
		finally {
			r.close();
		}
	}

	@Test
	public void lateEmptyResultIsPublished() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? Arrays.asList(A) : (call == 2)? p.sleep(150) : p.block());
		SeedRefresher r = refresher(provider, new Host[] {S}, 50, 100);

		try {
			assertArrayEquals(new Host[] {S, A}, start(r));
			await(() -> provider.calls.get() == 3);

			assertArrayEquals(new Host[] {S}, snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(warnings.toString(), 1, count(OVERRUN));
		}
		finally {
			r.close();
		}
	}

	@Test
	public void replaceLateEmptyResultEmptiesSnapshot() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? Arrays.asList(A) : (call == 2)? p.sleep(150) : p.block());
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryMergePolicy = SeedMergePolicy.REPLACE;
		policy.discoveryRefreshInterval = 50;
		policy.discoveryRefreshTimeout = 100;
		SeedRefresher r = new SeedRefresher(provider, policy, new Host[0], snapshot::set);
		r.validate(50);

		try {
			assertArrayEquals(new Host[] {A}, start(r));
			await(() -> provider.calls.get() == 3);

			assertArrayEquals(new Host[0], snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(warnings.toString(), 1, count(OVERRUN));
		}
		finally {
			r.close();
		}
	}

	@Test
	public void lateErrorKeepsLastGood() throws Exception {
		Provider provider = new Provider(true, (call, p) -> {
			if (call == 1) {
				return Arrays.asList(A);
			}
			if (call == 2) {
				p.sleep(150);
				throw new IllegalStateException("cloud API down");
			}
			return p.block(B);
		});
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			Host[] lastGood = start(r);
			await(() -> provider.calls.get() == 3);

			assertSame(lastGood, snapshot.get());
			assertEquals(1, r.getOverrunCount());
			assertEquals(warnings.toString(), 1, count(OVERRUN));
			assertEquals(warnings.toString(), 1, count(FAILED));
		}
		finally {
			r.close();
		}
	}

	@Test
	public void errorKeepsLastGood() throws Exception {
		Provider provider = new Provider(true, (call, p) -> {
			if (call == 1) {
				return Arrays.asList(A);
			}
			if (call == 2) {
				throw new IllegalStateException("cloud API down");
			}
			return p.block(B);
		});
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			Host[] lastGood = start(r);
			await(() -> provider.calls.get() == 3);

			assertSame(lastGood, snapshot.get());
			assertEquals(0, r.getOverrunCount());
			assertEquals(warnings.toString(), 1, count(FAILED));
		}
		finally {
			r.close();
		}
	}

	@Test
	public void overrunsNeverTrip() throws Exception {
		Provider provider = new Provider(true, (call, p) ->
			(call == 1)? Arrays.asList(A) : (call <= 5)? p.sleep(150, new Host("10.0.0.9", call)) : p.block());
		SeedRefresher r = refresher(provider, new Host[0], 50, 100);

		try {
			start(r);
			await(() -> provider.calls.get() == 6);

			assertEquals(4, r.getOverrunCount());
			assertFalse(r.isTripped());
			assertEquals(warnings.toString(), 4, count(OVERRUN));
			assertArrayEquals(new Host[] {new Host("10.0.0.9", 5)}, snapshot.get());
		}
		finally {
			r.close();
		}
	}

	@Test
	public void slowFirstRefreshUsesFallbackAndPublishesLate() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? p.sleep(1000, A) : p.block());
		SeedRefresher r = refresher(provider, new Host[] {S}, 60000, 200);

		try {
			long begin = System.nanoTime();
			Host[] seeds = start(r);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
			System.out.println("THREAD-MODE-STARTUP fallback after " + elapsed + "ms, refresh timeout 200ms");

			assertTrue(elapsed + "ms", elapsed >= 200 && elapsed <= 200 + BOUND);
			assertArrayEquals(new Host[] {S}, seeds);
			assertEquals(warnings.toString(), 1, count("Seed candidate provider "));
			assertTrue(warnings.toString(), warnings.stream().anyMatch(w ->
				w.contains("did not complete within the discovery refresh timeout 200ms")));

			await(() -> Arrays.equals(new Host[] {S, A}, snapshot.get()));
			assertEquals(1, r.getOverrunCount());
		}
		finally {
			r.close();
		}
	}

	@Test
	public void slowFirstRefreshClientUsesFallbackSeeds() throws Exception {
		Host late = new Host("127.0.0.1", 2);
		Provider provider = new Provider(true, (call, p) -> (call == 1)? p.sleep(1000, late) : p.block());
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshTimeout = 200;

		long begin = System.nanoTime();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

		try {
			Cluster cluster = client.getCluster();
			System.out.println("THREAD-MODE-CLIENT-STARTUP constructed in " + elapsed + "ms, refresh timeout 200ms");
			assertTrue(elapsed + "ms", elapsed <= 200 + BOUND);
			assertArrayEquals(new Host[] {DEAD_SEED}, cluster.getSeeds());
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, late}, cluster.getSeeds()));
		}
		finally {
			client.close();
		}
	}

	@Test
	public void slowFirstRefreshWithoutFallbackFails() throws Exception {
		Provider provider = new Provider(true, (call, p) -> p.sleep(500, A));
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshTimeout = 200;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		long begin = System.nanoTime();
		AerospikeException ae = assertThrows(AerospikeException.class, () -> new AerospikeClient(policy));
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		System.out.println("THREAD-MODE-STARTUP-FAILED after " + elapsed + "ms: " + ae.getMessage());

		assertEquals(ResultCode.SEED_DISCOVERY_FAILED, ae.getResultCode());
		assertTrue(elapsed + "ms", elapsed <= 200 + BOUND);
		await(() -> provider.closes.get() == 1);
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(1, provider.calls.get());
		assertEquals("discovery", provider.closeThread);
	}

	@Test
	public void reseedDoesNotWaitForRefreshInFlight() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? Arrays.asList(new Host("127.0.0.1", 11)) : p.block(B));
		ClientPolicy policy = policy(provider);

		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			Host[] seeds = cluster.getSeeds();
			await(() -> events.contains("refresh 2"));
			int from = events.indexOf("refresh 2");
			await(() -> events.subList(from, events.size()).stream().filter(e -> e.equals("seed 11")).count() >= 3);

			System.out.println("THREAD-MODE-RESEED events=" + events);
			assertEquals(2, provider.calls.get());
			assertEquals(1, provider.returned.get());
			assertSame(seeds, cluster.getSeeds());
			assertEquals(0, cluster.getNodes().length);
		}
		finally {
			client.close();
		}

		List<String> attempts = events.subList(0, events.indexOf("refresh 2"));
		assertEquals(Arrays.asList("refresh 1", "seed 1", "seed 11"), attempts.subList(0, 3));
	}

	@Test
	public void dnsTimeoutAboveRefreshTimeoutRejected() {
		DnsSeedCandidateProvider dns = DnsSeedCandidateProvider.builder().names("a.example.com").timeout(300)
			.servers("127.0.0.1:1").build();
		ClientPolicy policy = policy(dns);
		policy.discoveryRefreshTimeout = 200;

		AerospikeException ae = assertThrows(AerospikeException.class, () -> new AerospikeClient(policy, DEAD_SEED));
		assertEquals("DNS seed provider timeout 300 must be less or equal to the discovery refresh timeout 200",
			ae.getBaseMessage());

		ClientPolicy ok = new ClientPolicy();
		ok.discoveryRefreshTimeout = 300;
		new SeedRefresher(dns, ok, snapshot::set).validate(1000);

		ClientPolicy tend = new ClientPolicy();
		tend.discoveryExecutionMode = DiscoveryExecutionMode.TEND;
		tend.discoveryRefreshTimeout = 200;
		tend.discoveryTendRefreshDeadline = 300;
		new SeedRefresher(dns, tend, snapshot::set).validate(1000);
	}

	@Test
	public void rejectInvalidRefreshTimeout() {
		for (int timeout : new int[] {0, -1}) {
			ClientPolicy policy = policy(new Provider(true, (call, p) -> Arrays.asList(DEAD_SEED)));
			policy.discoveryRefreshTimeout = timeout;

			AerospikeException ae = assertThrows(AerospikeException.class, () -> new AerospikeClient(policy, DEAD_SEED));
			assertEquals("Invalid discovery refresh timeout: " + timeout + ". Must be > 0", ae.getBaseMessage());
		}
	}

	@Test
	public void tendModeRunsOnTendWithNoThread() throws Exception {
		Provider provider = new Provider(true, (call, p) -> Arrays.asList(DEAD_SEED));
		ClientPolicy policy = policy(provider);
		policy.discoveryExecutionMode = DiscoveryExecutionMode.TEND;
		policy.discoveryRefreshInterval = 250;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			assertNull(client.getCluster().seedRefresher.getThread());
			await(() -> provider.calls.get() >= 3);
			assertEquals(Arrays.asList("tend"), newThreadNames(before));
		}
		finally {
			client.close();
		}
		assertEquals(Thread.currentThread().getName(), provider.threads.get(0));
		assertTrue(provider.threads.subList(1, provider.threads.size()).stream().allMatch(t -> t.equals("tend")));
	}

	@Test
	public void closeWaitsForRefreshInFlight() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? Arrays.asList(DEAD_SEED) : p.sleep(400, B));
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshInterval = 250;
		policy.discoveryRefreshTimeout = 1000;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		Thread thread = client.getCluster().seedRefresher.getThread();
		await(() -> provider.calls.get() == 2);

		long begin = System.nanoTime();
		client.close();
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		System.out.println("THREAD-MODE-CLOSE refresh in flight returned, close took " + elapsed + "ms");

		assertEquals(2, provider.returned.get());
		assertEquals(1, provider.closes.get());
		assertEquals("discovery", provider.closeThread);
		assertFalse(thread.isAlive());
		assertTrue(elapsed + "ms", elapsed < 1000);
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(2, provider.calls.get());
	}

	@Test
	public void closeWaitsAtMostRefreshTimeout() throws Exception {
		Provider provider = new Provider(true, (call, p) -> (call == 1)? Arrays.asList(DEAD_SEED) : p.block(B));
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshInterval = 250;
		policy.discoveryRefreshTimeout = 500;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		Thread thread = client.getCluster().seedRefresher.getThread();
		await(() -> provider.calls.get() == 2);

		long begin = System.nanoTime();
		client.close();
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
		System.out.println("THREAD-MODE-CLOSE refresh still blocked, close took " + elapsed + "ms, refresh timeout 500ms");

		assertTrue(elapsed + "ms", elapsed >= 500 && elapsed <= 500 + BOUND);
		assertTrue(thread.isAlive());
		assertEquals(0, provider.closes.get());

		provider.release.countDown();
		await(() -> ! thread.isAlive());
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(1, provider.closes.get());
		assertEquals("discovery", provider.closeThread);
		assertEquals(2, provider.calls.get());
	}

	@Test
	public void closeStopsIdleThread() throws Exception {
		Provider provider = new Provider(true, (call, p) -> Arrays.asList(DEAD_SEED));
		ClientPolicy policy = policy(provider);

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		Thread thread = client.getCluster().seedRefresher.getThread();
		assertTrue(thread.isAlive());

		client.close();
		await(() -> ! thread.isAlive());
		await(() -> newThreadNames(before).isEmpty());
		assertEquals(1, provider.closes.get());
	}

	private static List<String> newThreadNames(Set<Thread> before) {
		List<String> names = new ArrayList<>();

		for (Thread thread : new HashSet<>(Thread.getAllStackTraces().keySet())) {
			if (! before.contains(thread) && thread.isAlive()) {
				names.add(thread.getName());
			}
		}
		names.sort(null);
		return names;
	}

	static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue("condition not met in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
	}
}
