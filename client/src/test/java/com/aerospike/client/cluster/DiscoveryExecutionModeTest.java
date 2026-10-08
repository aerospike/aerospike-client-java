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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.StaticSeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

public class DiscoveryExecutionModeTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host REFRESHED = new Host("127.0.0.1", 2);

	private static ClientPolicy policy(SeedCandidateProvider provider, DiscoveryExecutionMode mode) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.seedCandidateProvider = provider;
		policy.discoveryExecutionMode = mode;
		return policy;
	}

	@Test
	public void policyDefaults() {
		ClientPolicy policy = new ClientPolicy();
		assertNull(policy.seedCandidateProvider);
		assertEquals(DiscoveryExecutionMode.THREAD, policy.discoveryExecutionMode);
		assertEquals(30000, policy.discoveryRefreshInterval);
		assertEquals(100, policy.discoveryTendRefreshDeadline);
		assertEquals(10000, policy.discoveryRefreshTimeout);
	}

	@Test
	public void policySettersAndCopy() {
		SeedCandidateProvider provider = new StaticSeedCandidateProvider(DEAD_SEED);
		ClientPolicy policy = new ClientPolicy();
		policy.setSeedCandidateProvider(provider);
		policy.setDiscoveryExecutionMode(DiscoveryExecutionMode.TEND);
		policy.setDiscoveryRefreshInterval(5000);
		policy.setDiscoveryTendRefreshDeadline(50);
		policy.setDiscoveryRefreshTimeout(2000);

		ClientPolicy copy = new ClientPolicy(policy);
		assertSame(provider, copy.seedCandidateProvider);
		assertEquals(DiscoveryExecutionMode.TEND, copy.discoveryExecutionMode);
		assertEquals(5000, copy.discoveryRefreshInterval);
		assertEquals(50, copy.discoveryTendRefreshDeadline);
		assertEquals(2000, copy.discoveryRefreshTimeout);
	}

	@Test
	public void rejectRefreshIntervalBelowTendInterval() {
		for (DiscoveryExecutionMode mode : DiscoveryExecutionMode.values()) {
			ClientPolicy policy = policy(new TestSeedProvider(true, true, DEAD_SEED), mode);
			policy.tendInterval = 1000;
			policy.discoveryRefreshInterval = 999;

			AerospikeException ae = assertThrows(AerospikeException.class,
				() -> new AerospikeClient(policy, DEAD_SEED));
			assertTrue(ae.getMessage(), ae.getMessage().contains("Discovery refresh interval 999"));
		}
	}

	@Test
	public void allowRefreshIntervalBelowTendIntervalWithoutPeriodicRefresh() {
		ClientPolicy policy = policy(null, DiscoveryExecutionMode.THREAD);
		policy.tendInterval = 1000;
		policy.discoveryRefreshInterval = 999;

		new AerospikeClient(policy, DEAD_SEED).close();
	}

	@Test
	public void rejectInvalidTendRefreshDeadline() {
		for (int deadline : new int[] {0, -1, 501}) {
			ClientPolicy policy = policy(new TestSeedProvider(true, true, DEAD_SEED), DiscoveryExecutionMode.TEND);
			policy.tendInterval = 1000;
			policy.discoveryTendRefreshDeadline = deadline;

			AerospikeException ae = assertThrows(AerospikeException.class,
				() -> new AerospikeClient(policy, DEAD_SEED));
			assertTrue(ae.getMessage(), ae.getMessage().contains("Invalid discovery tend refresh deadline: " + deadline));
		}

		ClientPolicy policy = policy(null, DiscoveryExecutionMode.TEND);
		policy.tendInterval = 1000;
		policy.discoveryTendRefreshDeadline = 501;
		assertThrows(AerospikeException.class, () -> new AerospikeClient(policy, DEAD_SEED));

		policy.discoveryTendRefreshDeadline = 500;
		new AerospikeClient(policy, DEAD_SEED).close();
	}

	@Test
	public void rejectTendModeWithoutDeadlineSupport() {
		ClientPolicy policy = policy(new TestSeedProvider(false, false, DEAD_SEED), DiscoveryExecutionMode.TEND);

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AerospikeClient(policy, DEAD_SEED));
		assertTrue(ae.getMessage(), ae.getMessage().contains("supports a deadline"));

		policy.discoveryExecutionMode = DiscoveryExecutionMode.THREAD;
		new AerospikeClient(policy, DEAD_SEED).close();
	}

	@Test
	public void noPeriodicRefreshStartsNoThread() throws Exception {
		for (DiscoveryExecutionMode mode : DiscoveryExecutionMode.values()) {
			for (TestSeedProvider provider : new TestSeedProvider[] {null, new TestSeedProvider(false, true, DEAD_SEED)}) {
				ClientPolicy policy = policy(provider, mode);
				policy.discoveryRefreshInterval = 250;

				Set<Thread> before = Thread.getAllStackTraces().keySet();
				AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

				try {
					assertEquals(List.of("tend"), newThreadNames(before));
					Cluster cluster = client.getCluster();
					int tends = cluster.getInvalidNodeCount();
					await(() -> cluster.getInvalidNodeCount() >= tends + 3);

					assertEquals(List.of("tend"), newThreadNames(before));
					assertNull(cluster.seedRefresher.getThread());
					assertArrayEquals(new Host[] {DEAD_SEED}, cluster.getSeeds());

					if (provider != null) {
						assertEquals(1, provider.calls.get());
					}
				}
				finally {
					client.close();
				}
			}
		}
	}

	@Test
	public void threadModeStartsThreadStoppedByClose() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, DEAD_SEED);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClientPolicy policy = policy(provider, DiscoveryExecutionMode.THREAD);
		policy.discoveryRefreshInterval = 250;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		Cluster cluster = client.getCluster();
		Thread thread = cluster.seedRefresher.getThread();

		try {
			assertNotNull(thread);
			assertTrue(thread.isDaemon());
			assertEquals(List.of("discovery", "tend"), newThreadNames(before));
			await(() -> cluster.getSeeds()[0].equals(REFRESHED));
		}
		finally {
			client.close();
		}
		thread.join(5000);
		assertFalse(thread.isAlive());
	}

	@Test
	public void threadModeBlockedProviderNeverBlocksTend() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, DEAD_SEED);
		ClientPolicy policy = policy(provider, DiscoveryExecutionMode.THREAD);
		policy.discoveryRefreshInterval = 250;

		CountDownLatch block = new CountDownLatch(1);
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		Cluster cluster = client.getCluster();
		Thread thread = cluster.seedRefresher.getThread();

		try {
			provider.block = block;
			await(() -> provider.calls.get() >= 2);

			int tends = cluster.getInvalidNodeCount();
			await(() -> cluster.getInvalidNodeCount() >= tends + 5);
			assertEquals(2, provider.calls.get());
			assertArrayEquals(new Host[] {DEAD_SEED}, cluster.getSeeds());

			long begin = System.nanoTime();
			client.close();
			assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin) < 1000);
		}
		finally {
			client.close();
			block.countDown();
		}
		thread.join(5000);
		assertFalse(thread.isAlive());
		assertArrayEquals(new Host[] {DEAD_SEED}, cluster.getSeeds());
	}

	@Test
	public void tendModeRefreshesSeedsOnTend() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClientPolicy policy = policy(provider, DiscoveryExecutionMode.TEND);
		policy.discoveryRefreshInterval = 500;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			assertEquals(List.of("tend"), newThreadNames(before));
			assertNull(cluster.seedRefresher.getThread());
			await(() -> cluster.getSeeds()[0].equals(REFRESHED));
		}
		finally {
			client.close();
		}
	}

	@Test
	public void tendModeTripScopedToClientInstance() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClientPolicy policy = policy(provider, DiscoveryExecutionMode.TEND);
		policy.discoveryRefreshInterval = 250;
		policy.discoveryTendRefreshDeadline = 50;

		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		Cluster cluster = client.getCluster();

		try {
			await(() -> cluster.getSeeds()[0].equals(REFRESHED));
			provider.sleepMillis = 100;
			await(() -> cluster.seedRefresher.isTripped());
			assertEquals(SeedRefresher.MAX_OVERRUNS, cluster.seedRefresher.getOverrunCount());
			assertArrayEquals(new Host[] {DEAD_SEED}, cluster.getSeeds());

			int calls = provider.calls.get();
			int tends = cluster.getInvalidNodeCount();
			await(() -> cluster.getInvalidNodeCount() >= tends + 3);
			assertEquals(calls, provider.calls.get());
		}
		finally {
			client.close();
		}

		provider.sleepMillis = 0;
		TestSeedProvider fresh = new TestSeedProvider(true, true);
		fresh.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		policy.seedCandidateProvider = fresh;
		client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster freshCluster = client.getCluster();
			assertFalse(freshCluster.seedRefresher.isTripped());
			assertEquals(0, freshCluster.seedRefresher.getOverrunCount());
			await(() -> freshCluster.getSeeds()[0].equals(REFRESHED));
			assertFalse(freshCluster.seedRefresher.isTripped());
		}
		finally {
			client.close();
		}
	}

	@Test
	public void refreshedSeedsGetDefaultTlsName() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClientPolicy policy = policy(provider, DiscoveryExecutionMode.TEND);
		policy.discoveryRefreshInterval = 250;
		policy.tlsPolicy = new TlsPolicy();

		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			await(() -> cluster.getSeeds()[0].port == REFRESHED.port);
			assertEquals(REFRESHED.name, cluster.getSeeds()[0].tlsName);
		}
		finally {
			client.close();
		}
	}

	@Test
	public void concurrentRefreshNeverExposesPartialList() throws Exception {
		AerospikeClient client = new AerospikeClient(policy(null, DiscoveryExecutionMode.THREAD), DEAD_SEED);
		Cluster cluster = client.getCluster();

		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> {
			List<Host> list = new ArrayList<>();

			for (int i = 0; i <= call % 7; i++) {
				list.add(new Host("10.0.0." + (call % 200), call));
			}
			return list;
		};

		ClientPolicy refreshPolicy = new ClientPolicy();
		refreshPolicy.discoveryRefreshInterval = 0;
		refreshPolicy.discoveryRefreshTimeout = 60000;
		SeedRefresher refresher = new SeedRefresher(provider, refreshPolicy, cluster::setSeeds);

		AtomicBoolean running = new AtomicBoolean(true);
		AtomicReference<String> failure = new AtomicReference<>();
		Set<Integer> observed = ConcurrentHashMap.newKeySet();
		Thread[] readers = new Thread[4];

		for (int r = 0; r < readers.length; r++) {
			readers[r] = new Thread(() -> {
				while (running.get()) {
					Host[] seeds = cluster.getSeeds();
					int call = seeds[0].port;

					if (call == DEAD_SEED.port) {
						continue;
					}

					observed.add(call);

					if (seeds.length != call % 7 + 1) {
						failure.compareAndSet(null, "length " + seeds.length + " for refresh " + call);
					}

					for (Host host : seeds) {
						if (host == null || host.port != call || ! host.name.equals("10.0.0." + (call % 200))) {
							failure.compareAndSet(null, "mixed host " + host + " in refresh " + call);
						}
					}
				}
			});
			readers[r].start();
		}

		try {
			refresher.start(cluster.getSeeds());
			await(() -> provider.calls.get() >= 20000);
		}
		finally {
			refresher.close();
			running.set(false);

			for (Thread reader : readers) {
				reader.join(5000);
			}
			client.close();
		}

		assertNull(failure.get(), failure.get());
		assertTrue("observed " + observed.size(), observed.size() > 10);
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

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue("condition not met in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
	}
}
