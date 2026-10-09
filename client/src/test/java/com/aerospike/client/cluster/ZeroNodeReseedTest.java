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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;

public class ZeroNodeReseedTest {
	private static final Host DEAD_SEED = new Host("127.0.0.1", 1);
	private static final String SEED_PREFIX = "Seed 127.0.0.1 ";

	private final List<String> events = new ArrayList<>();

	@Before
	public void setUp() {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN && message.startsWith(SEED_PREFIX)) {
				String port = message.substring(SEED_PREFIX.length(), message.indexOf(' ', SEED_PREFIX.length()));
				event("seed " + port);
			}
		});
	}

	@After
	public void tearDown() {
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private void event(String event) {
		synchronized (events) {
			events.add(event);
		}
	}

	private List<String> events() {
		synchronized (events) {
			return new ArrayList<>(events);
		}
	}

	private static ClientPolicy policy(TestSeedProvider provider, int refreshInterval) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		policy.tendInterval = 250;
		policy.discoveryRefreshInterval = refreshInterval;
		policy.seedCandidateProvider = provider;
		return policy;
	}

	@Test
	public void reseedUsesSnapshotHeldAtStartOfTend() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true);
		provider.result = call -> {
			event("refresh " + call);
			return List.of(new Host("127.0.0.1", 10 + call));
		};

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		AerospikeClient client = new AerospikeClient(policy(provider, 250), DEAD_SEED);

		try {
			await(() -> provider.calls.get() >= 4);
			assertEquals(List.of("tend"), newThreadNames(before));
		}
		finally {
			client.close();
		}

		List<String> expected = List.of(
			"refresh 1",
			"seed 1", "seed 11",
			"seed 1", "seed 11", "refresh 2",
			"seed 1", "seed 12", "refresh 3",
			"seed 1", "seed 13", "refresh 4");
		assertEquals(expected, events().subList(0, expected.size()));
	}

	@Test
	public void reseedDoesNotWaitForRefreshInterval() throws Exception {
		TestSeedProvider provider = new TestSeedProvider(true, true, new Host("127.0.0.1", 11));

		Set<Thread> before = Thread.getAllStackTraces().keySet();
		long begin = System.nanoTime();
		AerospikeClient client = new AerospikeClient(policy(provider, 60000), DEAD_SEED);

		try {
			Cluster cluster = client.getCluster();
			Host[] seeds = cluster.getSeeds();
			await(() -> events().size() >= 8);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			assertTrue("three re-seeds took " + elapsed + "ms", elapsed < 5000);
			assertEquals(1, provider.calls.get());
			assertSame(seeds, cluster.getSeeds());
			assertEquals(0, cluster.getNodes().length);
			assertEquals(List.of("tend"), newThreadNames(before));
		}
		finally {
			client.close();
		}

		List<String> attempts = events();

		for (int i = 0; i < attempts.size(); i++) {
			assertEquals(i % 2 == 0 ? "seed 1" : "seed 11", attempts.get(i));
		}
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
