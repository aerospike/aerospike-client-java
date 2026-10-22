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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.configuration.serializers.staticconfig.StaticDiscoveryConfig;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.StubDnsServer;
import com.aerospike.client.policy.ClientPolicy;

public class DnsSeedRefresherTest {
	private static final Host[] INIT = new Host[] {new Host("10.0.0.1", 3000)};

	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private StubDnsServer dns;

	@Before
	public void setUp() throws Exception {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
		});
		snapshot.set(INIT);
		dns = new StubDnsServer();
	}

	@After
	public void tearDown() throws Exception {
		dns.close();
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private DnsSeedCandidateProvider provider(int timeout) {
		return DnsSeedCandidateProvider.builder().names("a.example.com", "b.example.com").timeout(timeout)
			.servers(dns.address()).build();
	}

	private static ClientPolicy offline() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		return policy;
	}

	@Test
	public void nxdomainOnEveryNameKeepsPreviousSeedList() throws Exception {
		dns.a("a.example.com", "10.0.0.2").a("b.example.com", "10.0.0.3");
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryRefreshInterval = 1000;
		SeedRefresher r = new SeedRefresher(provider(80), policy, snapshot::set);
		r.validate(1000);
		r.start(INIT);

		r.tend(1, 1000);
		Host[] lastGood = snapshot.get();
		assertArrayEquals(new Host[] {new Host("10.0.0.2", 3000), new Host("10.0.0.3", 3000)}, lastGood);

		dns.clear();
		r.tend(2, 1000);
		System.out.println("DNS-REFRESHER-NXDOMAIN snapshot=" + List.of(snapshot.get()) + " warnings=" + warnings);

		assertSame(lastGood, snapshot.get());
		assertEquals(warnings.toString(), 1, warnings.size());
		assertTrue(warnings.get(0), warnings.get(0).startsWith("Seed candidate refresh failed: "));
		assertTrue(warnings.get(0), warnings.get(0).contains("DNS seed discovery failed for every name: " +
			"a.example.com: NXDOMAIN; b.example.com: NXDOMAIN"));
		assertEquals(0, r.getOverrunCount());
		assertFalse(r.isTripped());

		dns.a("a.example.com", "10.0.0.4");
		r.tend(3, 1000);
		assertArrayEquals(new Host[] {new Host("10.0.0.4", 3000)}, snapshot.get());
	}

	@Test
	public void validateRejectsTimeoutAboveTendDeadline() {
		ClientPolicy policy = new ClientPolicy();
		SeedRefresher r = new SeedRefresher(provider(150), policy, snapshot::set);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> r.validate(1000));
		System.out.println("DNS-VALIDATE " + ae.getBaseMessage());
		assertEquals("DNS seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100",
			ae.getBaseMessage());

		new SeedRefresher(provider(100), policy, snapshot::set).validate(1000);

		policy.discoveryTendRefreshDeadline = 200;
		new SeedRefresher(provider(150), policy, snapshot::set).validate(1000);
	}

	@Test
	public void providerNameIsDns() {
		assertEquals(StaticDiscoveryConfig.DNS, SeedRefresher.providerName(provider(80)));
	}

	@Test
	public void timeoutAboveTendDeadlineFailsClientConstruction() throws Exception {
		ClientPolicy policy = offline();
		policy.seedCandidateProvider = provider(150);
		dns.a("a.example.com", "127.0.0.1");

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AerospikeClient(policy, new Host("127.0.0.1", 1)).close());
		assertEquals("DNS seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100",
			ae.getBaseMessage());
	}

	@Test
	public void initFailureWithoutStaticSeedFailsConstruction() {
		ClientPolicy policy = offline();
		policy.seedCandidateProvider = provider(80);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> new AerospikeClient(policy).close());
		System.out.println("DNS-INIT-NO-SEED " + ae.getMessage() + " warnings=" + warnings);

		assertEquals(ResultCode.SEED_DISCOVERY_FAILED, ae.getResultCode());
		assertTrue(ae.getMessage(), ae.getMessage().contains("Seed candidate provider dns failed and no seed host is left"));
		assertTrue(warnings.toString(), warnings.stream().anyMatch(m -> m.startsWith("Seed candidate provider dns failed: ") &&
			m.contains("DNS seed discovery failed for every name: a.example.com: NXDOMAIN; b.example.com: NXDOMAIN")));
	}

	@Test
	public void initFailureWithStaticSeedContinues() {
		ClientPolicy policy = offline();
		policy.seedCandidateProvider = provider(80);

		try (AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1))) {
			System.out.println("DNS-INIT-STATIC-SEED seeds=" + List.of(client.getCluster().getSeeds()) + " warnings=" + warnings);
			assertArrayEquals(new Host[] {new Host("127.0.0.1", 1)}, client.getCluster().getSeeds());
			assertTrue(warnings.toString(), warnings.stream().anyMatch(m -> m.startsWith("Seed candidate provider dns failed: ")));
		}
	}

	@Test
	public void initResolvesSeedsAsIpLiterals() throws Exception {
		dns.a("a.example.com", "127.0.0.1").srv("b.example.com", 1, 1, 1, "x.example.com");
		ClientPolicy policy = offline();
		policy.seedCandidateProvider = DnsSeedCandidateProvider.builder().names("a.example.com").port(2)
			.servers(dns.address()).build();

		try (AerospikeClient client = new AerospikeClient(policy, new Host("127.0.0.1", 1))) {
			Host[] seeds = client.getCluster().getSeeds();

			assertArrayEquals(new Host[] {new Host("127.0.0.1", 1), new Host("127.0.0.1", 2)}, seeds);
		}
	}
}
