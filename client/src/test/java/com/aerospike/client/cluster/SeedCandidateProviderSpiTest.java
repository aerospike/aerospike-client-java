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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.discovery.SeedCandidateProvider;
import com.aerospike.client.discovery.SeedMergePolicy;
import com.aerospike.client.discovery.StaticSeedCandidateProvider;
import com.aerospike.client.policy.ClientPolicy;

public class SeedCandidateProviderSpiTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final Host OTHER_SEED = new Host("127.0.0.1", 2);

	private final List<String> warnings = new CopyOnWriteArrayList<>();

	static final class Provider implements SeedCandidateProvider {
		final List<SeedCandidateContext> contexts = new CopyOnWriteArrayList<>();
		final AtomicInteger closes = new AtomicInteger();
		private final boolean periodic;
		volatile RuntimeException refreshError;
		volatile RuntimeException closeError;
		volatile int callsAtClose = -1;

		Provider(boolean periodic) {
			this.periodic = periodic;
		}

		@Override
		public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
			contexts.add(ctx);

			if (refreshError != null) {
				throw refreshError;
			}
			return Collections.emptyList();
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
			callsAtClose = contexts.size();
			closes.incrementAndGet();

			if (closeError != null) {
				throw closeError;
			}
		}
	}

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
		});
	}

	@After
	public void tearDown() {
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private static ClientPolicy policy(SeedCandidateProvider provider) {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryExecutionMode = DiscoveryExecutionMode.TEND;
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.discoveryRefreshInterval = 500;
		policy.seedCandidateProvider = provider;
		return policy;
	}

	@Test
	public void providerFailureAtInitKeepsStaticSeeds() throws Exception {
		for (SeedMergePolicy mergePolicy : SeedMergePolicy.values()) {
			Provider provider = new Provider(false);
			provider.refreshError = new IllegalStateException("cloud API down");
			ClientPolicy policy = policy(provider);
			policy.discoveryMergePolicy = mergePolicy;
			warnings.clear();

			try (AerospikeClient client = new AerospikeClient(policy, DEAD_SEED)) {
				assertArrayEquals(new Host[] {DEAD_SEED}, client.getCluster().getSeeds());
				assertEquals(1, provider.contexts.size());
				assertTrue(mergePolicy + " " + warnings, warnings.stream().anyMatch(m ->
					m.startsWith("Seed candidate provider " + Provider.class.getName() + " failed") &&
					m.contains("cloud API down")));
			}
		}
	}

	@Test
	public void providerFailureAtInitWithNoSeedFails() {
		Provider provider = new Provider(false);
		IllegalStateException error = new IllegalStateException("cloud API down");
		provider.refreshError = error;

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AerospikeClient(policy(provider)));

		System.out.println("SEED-DISCOVERY-FAILED " + ae.getMessage());
		assertEquals(ResultCode.SEED_DISCOVERY_FAILED, ae.getResultCode());
		assertTrue(ae.getMessage(), ae.getMessage().contains(Provider.class.getName()));
		assertTrue(ae.getMessage(), ae.getMessage().contains("cloud API down"));
		assertSame(error, ae.getCause());
		assertTrue(warnings.toString(), warnings.stream().anyMatch(m ->
			m.startsWith("Seed candidate provider " + Provider.class.getName() + " failed")));
	}

	@Test
	public void emptyResultWithNoSeedIsNotDiscoveryFailure() {
		Provider provider = new Provider(false);

		try (AerospikeClient client = new AerospikeClient(policy(provider))) {
			assertEquals(0, client.getCluster().getSeeds().length);
		}
	}

	@Test
	public void contextAtInitAndOnTend() throws Exception {
		Provider provider = new Provider(true);
		ClientPolicy policy = policy(provider);
		policy.clusterName = "ctx-cluster";

		try (AerospikeClient client = new AerospikeClient(policy, DEAD_SEED, OTHER_SEED)) {
			await(() -> provider.contexts.size() >= 3);
		}

		List<SeedCandidateContext> contexts = new ArrayList<>(provider.contexts);
		long previous = -1;

		for (int i = 0; i < contexts.size(); i++) {
			SeedCandidateContext ctx = contexts.get(i);

			assertEquals("ctx-cluster", ctx.clusterName);
			assertEquals(Arrays.asList(DEAD_SEED, OTHER_SEED), ctx.configuredSeeds);

			if (i == 0) {
				assertEquals(0, ctx.tendIteration);
			}
			else {
				assertTrue(contexts.toString(), ctx.tendIteration > previous);
				assertEquals(contexts.toString(), 0, ctx.tendIteration % 2);
			}
			previous = ctx.tendIteration;
		}
		assertThrows(UnsupportedOperationException.class, () -> contexts.get(0).configuredSeeds.add(DEAD_SEED));
	}

	@Test
	public void contextClusterNameNullWhenNotSet() {
		Provider provider = new Provider(false);

		try (AerospikeClient client = new AerospikeClient(policy(provider), DEAD_SEED)) {
			assertEquals(null, provider.contexts.get(0).clusterName);
			assertEquals(0, provider.contexts.get(0).tendIteration);
		}
	}

	@Test
	public void supportsDeadlineDefaultsToFalse() {
		SeedCandidateProvider provider = new SeedCandidateProvider() {
			@Override
			public List<Host> refreshSeedCandidates(SeedCandidateContext ctx) {
				return List.of();
			}

			@Override
			public boolean needsPeriodicRefresh() {
				return true;
			}
		};
		assertFalse(provider.supportsDeadline());

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AerospikeClient(policy(provider), DEAD_SEED));
		assertEquals("A seed candidate provider that needs periodic refresh must support a deadline in " +
			"discovery execution mode tend", ae.getBaseMessage());

		ClientPolicy policy = policy(provider);
		policy.discoveryExecutionMode = DiscoveryExecutionMode.THREAD;
		new AerospikeClient(policy, DEAD_SEED).close();
	}

	@Test
	public void hostWithPortOutOfRangeIsDropped() {
		Host low = new Host("10.0.0.1", 0);
		Host min = new Host("10.0.0.2", 1);
		Host high = new Host("10.0.0.3", 65536);
		Host max = new Host("10.0.0.4", 65535);
		TestSeedProvider provider = new TestSeedProvider(true, true, low, min, high, max);
		ClientPolicy policy = policy(provider);
		policy.discoveryMergePolicy = SeedMergePolicy.REPLACE;
		AtomicReference<Host[]> snapshot = new AtomicReference<>();
		SeedRefresher r = new SeedRefresher(provider, policy, new Host[0], snapshot::set);

		Host[] seeds = r.discover();
		assertArrayEquals(new Host[] {min, max}, seeds);
		r.start(seeds);

		provider.result = call -> List.of(high, max, low);
		r.tend(2, 250);
		assertArrayEquals(new Host[] {max}, snapshot.get());

		String prefix = "Seed candidate provider " + TestSeedProvider.class.getName() + " returned host ";
		assertEquals(warnings.toString(), List.of(
			prefix + "10.0.0.1 with port 0 outside 1-65535. Host dropped",
			prefix + "10.0.0.3 with port 65536 outside 1-65535. Host dropped",
			prefix + "10.0.0.3 with port 65536 outside 1-65535. Host dropped",
			prefix + "10.0.0.1 with port 0 outside 1-65535. Host dropped"), warnings);
	}

	@Test
	public void builtInProviderHostWithPortOutOfRangeIsDropped() {
		Host valid = new Host("10.0.0.2", 3000);
		ClientPolicy policy = policy(null);
		policy.discoveryMergePolicy = SeedMergePolicy.DISCOVERY_ONLY;
		SeedRefresher r = new SeedRefresher(new StaticSeedCandidateProvider(new Host("10.0.0.1", 65536), valid),
			policy, new Host[0], hosts -> {});

		assertArrayEquals(new Host[] {valid}, r.discover());
		assertEquals(warnings.toString(), List.of(
			"Seed candidate provider static returned host 10.0.0.1 with port 65536 outside 1-65535. Host dropped"),
			warnings);
	}

	@Test
	public void closeCalledOnceAfterRefreshStops() throws Exception {
		Provider provider = new Provider(true);
		AerospikeClient client = new AerospikeClient(policy(provider), DEAD_SEED);
		Cluster cluster = client.getCluster();
		await(() -> provider.contexts.size() >= 2);

		client.close();
		await(() -> provider.closes.get() == 1);
		assertEquals(provider.callsAtClose, provider.contexts.size());

		int tends = cluster.getInvalidNodeCount();
		Thread.sleep(1000);
		client.close();

		assertEquals(1, provider.closes.get());
		assertEquals(provider.callsAtClose, provider.contexts.size());
		assertTrue(cluster.getInvalidNodeCount() <= tends + 1);
	}

	@Test
	public void closeExceptionIsLoggedNotThrown() throws Exception {
		Provider provider = new Provider(false);
		provider.closeError = new IllegalStateException("close boom");

		AerospikeClient client = new AerospikeClient(policy(provider), DEAD_SEED);
		client.close();

		await(() -> provider.closes.get() == 1);
		await(() -> warnings.stream().anyMatch(m -> m.contains("close boom")));
		assertTrue(warnings.toString(), warnings.stream().anyMatch(m ->
			m.startsWith("Seed candidate provider " + Provider.class.getName() + " close failed") &&
			m.contains("close boom")));
	}

	@Test
	public void sharedProviderClosedByEachClient() throws Exception {
		Provider provider = new Provider(false);
		AerospikeClient c1 = new AerospikeClient(policy(provider), DEAD_SEED);
		AerospikeClient c2 = new AerospikeClient(policy(provider), DEAD_SEED);

		c1.close();
		await(() -> provider.closes.get() == 1);
		c2.close();
		await(() -> provider.closes.get() == 2);
	}

	private static void await(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

		while (! condition.getAsBoolean()) {
			assertTrue("condition not met in time", System.nanoTime() < deadline);
			Thread.sleep(5);
		}
	}
}
