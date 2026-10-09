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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.StaticSeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

public class DiscoveryRefreshTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host REFRESHED = new Host("127.0.0.1", 2);

	private static ClientPolicy policy(SeedCandidateProvider provider) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.seedCandidateProvider = provider;
		return policy;
	}

	@Test
	public void policyDefaults() {
		ClientPolicy policy = new ClientPolicy();
		assertNull(policy.seedCandidateProvider);
		assertEquals(30000, policy.discoveryRefreshInterval);
		assertEquals(100, policy.discoveryTendRefreshDeadline);
	}

	@Test
	public void policySettersAndCopy() {
		SeedCandidateProvider provider = new StaticSeedCandidateProvider(DEAD_SEED);
		ClientPolicy policy = new ClientPolicy();
		policy.setSeedCandidateProvider(provider);
		policy.setDiscoveryRefreshInterval(5000);
		policy.setDiscoveryTendRefreshDeadline(50);

		ClientPolicy copy = new ClientPolicy(policy);
		assertSame(provider, copy.seedCandidateProvider);
		assertEquals(5000, copy.discoveryRefreshInterval);
		assertEquals(50, copy.discoveryTendRefreshDeadline);
	}

	@Test
	public void rejectRefreshIntervalBelowTendInterval() {
		ClientPolicy policy = policy(new TestSeedProvider(true, true, DEAD_SEED));
		policy.tendInterval = 1000;
		policy.discoveryRefreshInterval = 999;

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AerospikeClient(policy, DEAD_SEED));
		assertTrue(ae.getMessage(), ae.getMessage().contains("Discovery refresh interval 999"));
	}

	@Test
	public void allowRefreshIntervalBelowTendIntervalWithoutPeriodicRefresh() {
		ClientPolicy policy = policy(null);
		policy.tendInterval = 1000;
		policy.discoveryRefreshInterval = 999;

		new AerospikeClient(policy, DEAD_SEED).close();
	}

	@Test
	public void rejectInvalidTendRefreshDeadline() {
		for (int deadline : new int[] {0, -1, 501}) {
			ClientPolicy policy = policy(new TestSeedProvider(true, true, DEAD_SEED));
			policy.tendInterval = 1000;
			policy.discoveryTendRefreshDeadline = deadline;

			AerospikeException ae = assertThrows(AerospikeException.class,
				() -> new AerospikeClient(policy, DEAD_SEED));
			assertTrue(ae.getMessage(), ae.getMessage().contains("Invalid discovery tend refresh deadline: " + deadline));
		}

		ClientPolicy policy = policy(new TestSeedProvider(true, true, DEAD_SEED));
		policy.tendInterval = 1000;
		policy.discoveryTendRefreshDeadline = 500;
		new AerospikeClient(policy, DEAD_SEED).close();

		policy.seedCandidateProvider = null;
		policy.discoveryTendRefreshDeadline = 501;
		new AerospikeClient(policy, DEAD_SEED).close();
	}

	@Test
	public void rejectPeriodicProviderWithoutDeadlineSupport() {
		ClientPolicy policy = policy(new TestSeedProvider(true, false, DEAD_SEED));

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AerospikeClient(policy, DEAD_SEED));
		assertTrue(ae.getMessage(), ae.getMessage().contains("must support a deadline"));

		policy.seedCandidateProvider = new TestSeedProvider(false, false, DEAD_SEED);
		new AerospikeClient(policy, DEAD_SEED).close();
	}

	@Test
	public void noPeriodicRefreshStartsNoThread() throws Exception {
		for (TestSeedProvider provider : new TestSeedProvider[] {null, new TestSeedProvider(false, true, DEAD_SEED)}) {
			ClientPolicy policy = policy(provider);
			policy.discoveryRefreshInterval = 250;

			Set<Thread> before = Thread.getAllStackTraces().keySet();
			AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

			try {
				assertEquals(List.of("tend"), newThreadNames(before));
				Cluster cluster = client.getCluster();
				int tends = cluster.getInvalidNodeCount();
				await(() -> cluster.getInvalidNodeCount() >= tends + 3);

				assertEquals(List.of("tend"), newThreadNames(before));
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

	@Test
	public void periodicRefreshRunsOnTendWithNoNewThread() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshInterval = 500;

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			assertEquals(List.of("tend"), newThreadNames(before));
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, REFRESHED}, cluster.getSeeds()));
		}
		finally {
			client.close();
		}
	}

	@Test
	public void tripScopedToClientInstance() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(DEAD_SEED) : List.of(REFRESHED);
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshInterval = 250;
		policy.discoveryTendRefreshDeadline = 50;

		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);
		Cluster cluster = client.getCluster();

		try {
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, REFRESHED}, cluster.getSeeds()));
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
			await(() -> Arrays.equals(new Host[] {DEAD_SEED, REFRESHED}, freshCluster.getSeeds()));
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
		ClientPolicy policy = policy(provider);
		policy.discoveryRefreshInterval = 250;
		policy.tlsPolicy = new TlsPolicy();

		AerospikeClient client = new AerospikeClient(policy, DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			await(() -> cluster.getSeeds().length == 2);
			assertEquals(REFRESHED, cluster.getSeeds()[1]);
			assertEquals(REFRESHED.name, cluster.getSeeds()[1].tlsName);
		}
		finally {
			client.close();
		}
	}

	@Test
	public void concurrentRefreshNeverExposesPartialList() throws Exception {
		AerospikeClient client = new AerospikeClient(policy(null), DEAD_SEED);
		Cluster cluster = client.getCluster();

		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> {
			List<Host> list = new ArrayList<>();

			for (int i = 0; i <= call % 7; i++) {
				list.add(new Host("10.0.0." + i, call));
			}
			return list;
		};

		ClientPolicy refreshPolicy = new ClientPolicy();
		refreshPolicy.discoveryRefreshInterval = 1000;
		refreshPolicy.discoveryTendRefreshDeadline = 60000;
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

					for (int i = 0; i < seeds.length; i++) {
						Host host = seeds[i];

						if (host == null || host.port != call || ! host.name.equals("10.0.0." + i)) {
							failure.compareAndSet(null, "mixed host " + host + " in refresh " + call);
						}
					}
				}
			});
			readers[r].start();
		}

		refresher.start(cluster.getSeeds());
		Thread writer = new Thread(() -> {
			for (int tendCount = 1; running.get() && provider.calls.get() < 20000; tendCount++) {
				refresher.tend(tendCount, 1000);
			}
		});
		writer.start();

		try {
			await(() -> provider.calls.get() >= 20000);
		}
		finally {
			running.set(false);
			writer.join(5000);

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
