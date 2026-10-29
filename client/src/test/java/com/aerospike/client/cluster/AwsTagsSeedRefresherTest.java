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

import java.net.ServerSocket;
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
import com.aerospike.client.configuration.serializers.staticconfig.StaticDiscoveryConfig;
import com.aerospike.client.discovery.AwsEc2TagSeedCandidateProvider;
import com.aerospike.client.discovery.StubAwsServer;
import com.aerospike.client.discovery.StubAwsServer.Reply;
import com.aerospike.client.policy.ClientPolicy;

public class AwsTagsSeedRefresherTest {
	private static final Host[] INIT = new Host[] {new Host("10.0.0.1", 3000)};
	private static final int MARGIN = 50;

	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private final AtomicReference<Host[]> snapshot = new AtomicReference<>();
	private StubAwsServer stub;

	@Before
	public void setUp() throws Exception {
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN || level == Log.Level.ERROR) {
				warnings.add(message);
			}
		});
		snapshot.set(INIT);
		stub = new StubAwsServer();
	}

	@After
	public void tearDown() {
		stub.close();
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);
	}

	private AwsEc2TagSeedCandidateProvider provider(int timeout, String endpoint) {
		return AwsEc2TagSeedCandidateProvider.builder().region("us-east-1").tagFilter("aerospike-cluster", "prod-a")
			.credentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", null)
			.endpoint(endpoint).timeout(timeout).build();
	}

	private static ClientPolicy offline() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = false;
		policy.timeout = 100;
		return policy;
	}

	@Test
	public void validateRejectsTimeoutAboveTendDeadline() {
		ClientPolicy policy = new ClientPolicy();
		SeedRefresher r = new SeedRefresher(provider(150, stub.url()), policy, snapshot::set);

		AerospikeException ae = assertThrows(AerospikeException.class, () -> r.validate(1000));
		System.out.println("AWS-TAGS-VALIDATE " + ae.getBaseMessage());
		assertEquals("AWS EC2 seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100",
			ae.getBaseMessage());

		new SeedRefresher(provider(100, stub.url()), policy, snapshot::set).validate(1000);

		policy.discoveryTendRefreshDeadline = 200;
		new SeedRefresher(provider(150, stub.url()), policy, snapshot::set).validate(1000);
	}

	@Test
	public void providerNameIsAwsTags() {
		assertEquals(StaticDiscoveryConfig.AWS_TAGS, SeedRefresher.providerName(provider(80, stub.url())));
	}

	@Test
	public void timeoutAboveTendDeadlineFailsClientConstruction() {
		ClientPolicy policy = offline();
		policy.seedCandidateProvider = provider(150, stub.url());

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> new AerospikeClient(policy, new Host("127.0.0.1", 1)).close());
		assertEquals("AWS EC2 seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100",
			ae.getBaseMessage());
	}

	@Test
	public void runsOnTendAndPublishesInstances() {
		stub.ec2Default(new Reply(200, StubAwsServer.describe(null,
			StubAwsServer.instance("i-1", "10.0.0.2", null, null, null),
			StubAwsServer.instance("i-2", "10.0.0.3", null, null, null))));
		ClientPolicy policy = new ClientPolicy();
		SeedRefresher r = new SeedRefresher(provider(80, stub.url()), policy, snapshot::set);
		r.validate(1000);
		r.start(INIT);

		r.tend(30, 1000);
		assertArrayEquals(new Host[] {new Host("10.0.0.2", 3000), new Host("10.0.0.3", 3000)}, snapshot.get());

		Host[] lastGood = snapshot.get();
		stub.ec2Default(new Reply(403, StubAwsServer.error("UnauthorizedOperation",
			"You are not authorized to perform this operation.", "ea966190-f9aa-478e-9ede-example")));
		r.tend(60, 1000);
		System.out.println("AWS-TAGS-REFRESHER-ERROR snapshot=" + List.of(snapshot.get()) + " warnings=" + warnings);

		assertSame(lastGood, snapshot.get());
		assertEquals(warnings.toString(), 1, warnings.size());
		assertTrue(warnings.get(0), warnings.get(0).startsWith("Seed candidate refresh failed: "));
		assertTrue(warnings.get(0), warnings.get(0).contains("UnauthorizedOperation") &&
			warnings.get(0).contains("ea966190-f9aa-478e-9ede-example"));
		assertFalse(r.isTripped());
	}

	@Test
	public void clientConstructionSeedsFromProvider() {
		stub.ec2Default(new Reply(200, StubAwsServer.describe(null,
			StubAwsServer.instance("i-1", "127.0.0.1", null, null, null))));
		ClientPolicy policy = offline();
		policy.seedCandidateProvider = AwsEc2TagSeedCandidateProvider.builder().region("us-east-1")
			.tagFilter("aerospike-cluster", "prod-a").credentials("AKIDEXAMPLE", "secret", null)
			.endpoint(stub.url()).port(1).build();

		try (AerospikeClient client = new AerospikeClient(policy)) {
			System.out.println("AWS-TAGS-INIT seeds=" + List.of(client.getCluster().getSeeds()));
			assertArrayEquals(new Host[] {new Host("127.0.0.1", 1)}, client.getCluster().getSeeds());
		}
	}

	@Test
	public void unansweredStubAtTendDeadline() throws Exception {
		try (ServerSocket silent = StubAwsServer.silent()) {
			ClientPolicy policy = new ClientPolicy();
			policy.discoveryTendRefreshDeadline = 50;
			SeedRefresher r = new SeedRefresher(provider(50, "http://127.0.0.1:" + silent.getLocalPort()), policy,
				snapshot::set);
			r.validate(1000);
			r.start(INIT);
			int calls = SeedRefresher.MAX_OVERRUNS - 1;

			for (int i = 1; i <= calls; i++) {
				long begin = System.nanoTime();
				r.tend(30 * i, 1000);
				long elapsed = (System.nanoTime() - begin) / 1000000;
				System.out.println("AWS-TAGS-REFRESHER-BOUNDED call=" + i + " elapsed=" + elapsed + "ms deadline=50ms");
				assertTrue(elapsed + "ms", elapsed <= 50 + MARGIN);
			}
			assertSame(INIT, snapshot.get());
			assertEquals(warnings.toString(), calls, warnings.stream().filter(w ->
				w.startsWith("Seed candidate refresh failed: ") && w.contains("timed out after 50ms")).count());
		}
	}
}
