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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

public class SeedMergePolicyTest {
	private static final Host S1 = new Host("10.0.0.1", 3000);
	private static final Host S2 = new Host("10.0.0.2", 3000);
	private static final Host D1 = new Host("10.0.1.1", 3000);
	private static final Host D2 = new Host("10.0.1.2", 3000);
	private static final Host[] STATIC = new Host[] {S1, S2};
	private static final Host[] DISCOVERED = new Host[] {D1, new Host("10.0.0.2", "tls", 3000), D2, D1};
	private static final Host[] EMPTY = new Host[0];

	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private final AtomicInteger publishCount = new AtomicInteger();

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.ERROR);
	}

	@After
	public void tearDown() {
		Log.setLevel(Log.Level.INFO);
	}

	private SeedRefresher create(SeedMergePolicy mergePolicy, TestSeedProvider provider, Host[] staticSeeds) {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryMergePolicy = mergePolicy;
		policy.discoveryRefreshInterval = 1000;
		policy.discoveryTendRefreshDeadline = 20;
		return new SeedRefresher(provider, policy, staticSeeds, hosts -> {
			publishCount.incrementAndGet();
			snapshot.set(hosts);
		});
	}

	private Host[] init(SeedRefresher refresher, Host[] discovered) {
		Host[] seeds = refresher.merge(discovered, true);
		snapshot.set(seeds);
		refresher.start(seeds);
		return seeds;
	}

	@Test
	public void policyDefaultSetterAndCopy() {
		ClientPolicy policy = new ClientPolicy();
		assertEquals(SeedMergePolicy.MERGE, policy.discoveryMergePolicy);

		policy.setDiscoveryMergePolicy(SeedMergePolicy.DISCOVERY_ONLY);
		assertEquals(SeedMergePolicy.DISCOVERY_ONLY, policy.discoveryMergePolicy);
		assertEquals(SeedMergePolicy.DISCOVERY_ONLY, new ClientPolicy(policy).discoveryMergePolicy);

		policy.setDiscoveryMergePolicy(SeedMergePolicy.REPLACE);
		assertEquals(SeedMergePolicy.REPLACE, new ClientPolicy(policy).discoveryMergePolicy);
	}

	@Test
	public void mergeOrder() {
		SeedRefresher r = create(SeedMergePolicy.MERGE, new TestSeedProvider(true, true), STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1, D2}, r.merge(DISCOVERED, true));
		assertSame(S2, r.merge(DISCOVERED, true)[1]);
		assertArrayEquals(new Host[] {S1, S2, D1, D2}, r.merge(DISCOVERED, false));
		assertArrayEquals(STATIC, r.merge(EMPTY, true));
		assertArrayEquals(STATIC, r.merge(EMPTY, false));
	}

	@Test
	public void replaceOrder() {
		SeedRefresher r = create(SeedMergePolicy.REPLACE, new TestSeedProvider(true, true), STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1, D2}, r.merge(DISCOVERED, true));
		assertArrayEquals(new Host[] {D1, S2, D2}, r.merge(DISCOVERED, false));
		assertEquals("tls", r.merge(DISCOVERED, false)[1].tlsName);
		assertArrayEquals(STATIC, r.merge(EMPTY, true));
		assertArrayEquals(EMPTY, r.merge(EMPTY, false));
	}

	@Test
	public void discoveryOnlyOrder() {
		SeedRefresher r = create(SeedMergePolicy.DISCOVERY_ONLY, new TestSeedProvider(true, true), STATIC);
		assertArrayEquals(new Host[] {D1, S2, D2}, r.merge(DISCOVERED, true));
		assertArrayEquals(new Host[] {D1, S2, D2}, r.merge(DISCOVERED, false));
		assertArrayEquals(STATIC, r.merge(EMPTY, true));
		assertArrayEquals(STATIC, r.merge(EMPTY, false));
	}

	@Test
	public void staticSeedsCopiedFromCaller() {
		Host[] hosts = new Host[] {S1, S2};
		SeedRefresher r = create(SeedMergePolicy.MERGE, new TestSeedProvider(true, true), hosts);
		hosts[0] = D2;
		assertArrayEquals(STATIC, r.merge(EMPTY, false));
		assertArrayEquals(new Host[] {D2, S2}, hosts);
	}

	@Test
	public void mergeRefreshPublishesStaticThenDiscovered() {
		TestSeedProvider provider = new TestSeedProvider(true, true, D2);
		SeedRefresher r = create(SeedMergePolicy.MERGE, provider, STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1}, init(r, new Host[] {D1}));

		r.tend(1, 1000);
		assertArrayEquals(new Host[] {S1, S2, D2}, snapshot.get());

		provider.result = call -> List.of();
		r.tend(2, 1000);
		assertArrayEquals(STATIC, snapshot.get());
	}

	@Test
	public void replaceDropsStaticAfterInitAndKeepsPreviousOnEmpty() {
		TestSeedProvider provider = new TestSeedProvider(true, true, D2, S1);
		SeedRefresher r = create(SeedMergePolicy.REPLACE, provider, STATIC);
		assertArrayEquals(new Host[] {S1, S2, D1}, init(r, new Host[] {D1}));

		r.tend(1, 1000);
		Host[] replaced = snapshot.get();
		assertArrayEquals(new Host[] {D2, S1}, replaced);

		provider.result = call -> List.of();
		r.tend(2, 1000);
		assertSame(replaced, snapshot.get());
		assertEquals(1, publishCount.get());
	}

	@Test
	public void discoveryOnlyFallsBackToStaticOnEmpty() {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		SeedRefresher r = create(SeedMergePolicy.DISCOVERY_ONLY, provider, STATIC);
		assertArrayEquals(STATIC, init(r, EMPTY));

		provider.result = call -> List.of(D1, D2);
		r.tend(1, 1000);
		assertArrayEquals(new Host[] {D1, D2}, snapshot.get());

		provider.result = call -> List.of();
		r.tend(2, 1000);
		assertArrayEquals(STATIC, snapshot.get());
	}

	@Test
	public void failedEmptyOrOverrunRefreshNeverEmptiesSnapshot() {
		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			for (Host[] staticSeeds : new Host[][] {STATIC, EMPTY}) {
				TestSeedProvider provider = new TestSeedProvider(true, true, D1);
				SeedRefresher r = create(mergePolicy, provider, staticSeeds);
				init(r, new Host[] {D2});

				r.tend(1, 1000);
				Host[] previous = snapshot.get();
				String label = mergePolicy + " static " + staticSeeds.length;

				provider.error = new RuntimeException("fail");
				r.tend(2, 1000);
				assertSame(label, previous, snapshot.get());

				provider.error = null;
				provider.result = call -> null;
				r.tend(3, 1000);
				assertSame(label, previous, snapshot.get());

				provider.result = call -> List.of();
				r.tend(4, 1000);
				assertTrue(label, snapshot.get().length > 0);

				Host[] beforeOverrun = snapshot.get();
				provider.result = call -> List.of();
				provider.sleepMillis = 40;
				r.tend(5, 1000);
				assertSame(label, beforeOverrun, snapshot.get());
				assertTrue(label, snapshot.get().length > 0);
			}
		}
	}

	@Test
	public void tripRestoresMergedInitSnapshot() {
		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			TestSeedProvider provider = new TestSeedProvider(true, true, D2);
			SeedRefresher r = create(mergePolicy, provider, STATIC);
			Host[] initSeeds = init(r, new Host[] {D1});

			r.tend(1, 1000);
			provider.sleepMillis = 40;

			for (int i = 1; i <= SeedRefresher.MAX_OVERRUNS; i++) {
				r.tend(1 + i, 1000);
			}
			assertTrue(r.isTripped());
			assertSame(mergePolicy.toString(), initSeeds, snapshot.get());
		}
		assertArrayEquals(new Host[] {D1}, snapshot.get());
	}

	@Test
	public void mergeRunsOnlyWhenSnapshotPublished() {
		TestSeedProvider provider = new TestSeedProvider(true, true, D1);
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryRefreshInterval = 10000;
		SeedRefresher r = new SeedRefresher(provider, policy, STATIC, hosts -> {
			publishCount.incrementAndGet();
			snapshot.set(hosts);
		});
		Host[] initSeeds = init(r, EMPTY);

		for (int tendCount = 1; tendCount < 10; tendCount++) {
			r.tend(tendCount, 1000);
			assertSame(initSeeds, snapshot.get());
		}
		assertEquals(0, publishCount.get());

		r.tend(10, 1000);
		assertEquals(1, publishCount.get());
		assertArrayEquals(new Host[] {S1, S2, D1}, snapshot.get());
	}

	@Test
	public void noProviderSnapshotEqualsStaticHosts() {
		Host[] hosts = new Host[] {new Host("127.0.0.1", 3), new Host("127.0.0.1", 1), new Host("127.0.0.1", 2)};

		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			for (boolean tls : new boolean[] {false, true}) {
				ClientPolicy policy = new ClientPolicy();
				policy.failIfNotConnected = false;
				policy.timeout = 100;
				policy.discoveryMergePolicy = mergePolicy;

				if (tls) {
					policy.tlsPolicy = new TlsPolicy();
				}

				AerospikeClient client = new AerospikeClient(policy, hosts);

				try {
					Host[] seeds = client.getCluster().getSeeds();
					assertArrayEquals(mergePolicy.toString(), hosts, seeds);

					for (int i = 0; i < hosts.length; i++) {
						assertNull(hosts[i].tlsName);
						assertEquals(tls ? hosts[i].name : null, seeds[i].tlsName);
					}
				}
				finally {
					client.close();
				}
			}
		}
	}

	@Test
	public void clusterTripRestoresMergedInitSnapshot() throws Exception {
		Host deadSeed = new Host("127.0.0.1", 1);
		Host initDiscovered = new Host("127.0.0.1", 2);
		Host refreshed = new Host("127.0.0.1", 3);
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> (call == 1)? List.of(initDiscovered) : List.of(refreshed);
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.discoveryRefreshInterval = 250;
		policy.discoveryTendRefreshDeadline = 50;
		policy.seedCandidateProvider = provider;

		AerospikeClient client = new AerospikeClient(policy, deadSeed);

		try {
			Cluster cluster = client.getCluster();
			await(() -> Arrays.equals(new Host[] {deadSeed, refreshed}, cluster.getSeeds()));

			provider.sleepMillis = 100;
			await(() -> cluster.seedRefresher.isTripped());
			assertArrayEquals(new Host[] {deadSeed, initDiscovered}, cluster.getSeeds());
		}
		finally {
			client.close();
		}
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue("condition not met in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
	}
}
