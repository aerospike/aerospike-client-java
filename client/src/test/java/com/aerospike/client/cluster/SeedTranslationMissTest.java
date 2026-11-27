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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.AlternateAddressMissingTest.FakeNode;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;

/**
 * With translation.on_miss strict, a seed whose own advertised address has no translation is not
 * used. The next seed is tried, and connect fails with TRANSLATION_MISS only when no seed is left.
 */
public class SeedTranslationMissTest {
	private static final String UNMAPPED = "10.0.0.9:3000";

	private final List<String> warnings = new ArrayList<>();
	private final List<FakeNode> servers = new ArrayList<>();
	private AerospikeClient client;
	private Thread testThread;

	@Before
	public void setUp() {
		testThread = Thread.currentThread();
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN && Thread.currentThread() == testThread) {
				synchronized (warnings) {
					warnings.add(message);
				}
			}
		});
	}

	@After
	public void tearDown() throws Exception {
		Log.setCallback(null);
		Log.setLevel(Log.Level.INFO);

		if (client != null) {
			client.close();
		}

		for (FakeNode server : servers) {
			server.close();
		}
	}

	@Test
	public void strictSeedWithUnmappedAddressIsSkipped() throws Exception {
		FakeNode a = server("BB9A", UNMAPPED);
		FakeNode b = server("BB9B", null);
		b.serviceStd = "127.0.0.1:" + b.port();

		ClientPolicy policy = policy(TranslationMissMode.STRICT);
		policy.failIfNotConnected = false;

		client = new AerospikeClient(policy, seed(a), seed(b));

		assertEquals(1, client.getNodes().length);
		assertEquals("BB9B", client.getNodes()[0].getName());

		List<String> misses = misses();
		assertEquals(warnings().toString(), 1, misses.size());
		assertNames(misses.get(0), "Seed 127.0.0.1 " + a.port(), "BB9A", UNMAPPED, "static-map");
	}

	@Test
	public void strictSeedMissWithNoSeedLeftFailsConnect() throws Exception {
		FakeNode a = server("BB9A", UNMAPPED);

		AerospikeException.TranslationMiss e = assertThrows(AerospikeException.TranslationMiss.class,
			() -> new AerospikeClient(policy(TranslationMissMode.STRICT), seed(a)));

		assertEquals(ResultCode.TRANSLATION_MISS, e.getResultCode());
		assertNames(e.getMessage(), "BB9A", UNMAPPED, "standard", "static-map");
		assertEquals(1, misses().size());
	}

	@Test
	public void strictSeedMissOnLoadBalancerPathFailsConnect() throws Exception {
		FakeNode a = server("BB9A", UNMAPPED);
		ClientPolicy policy = policy(TranslationMissMode.STRICT);
		policy.ipMap = Map.of("0.0.0.0", "0.0.0.0");

		AerospikeException.TranslationMiss e = assertThrows(AerospikeException.TranslationMiss.class,
			() -> new AerospikeClient(policy, new Host("0.0.0.0", a.port())));

		assertNames(e.getMessage(), "BB9A", UNMAPPED, "static-map");
	}

	@Test
	public void strictSeedWithOneMappedAddressIsUsed() throws Exception {
		FakeNode a = server("BB9A", null);
		a.serviceStd = UNMAPPED + ",127.0.0.1:" + a.port();

		client = new AerospikeClient(policy(TranslationMissMode.STRICT), seed(a));

		assertEquals(1, client.getNodes().length);
		assertEquals("BB9A", client.getNodes()[0].getName());
		assertEquals(List.of(), misses());
	}

	@Test
	public void passThroughSeedMissKeepsSeed() throws Exception {
		FakeNode a = server("BB9A", UNMAPPED);

		client = new AerospikeClient(policy(TranslationMissMode.PASS_THROUGH), seed(a));

		assertEquals(1, client.getNodes().length);
		assertEquals("BB9A", client.getNodes()[0].getName());
		assertEquals(List.of(), misses());

		for (List<String> request : a.requests()) {
			assertFalse(request.toString(), request.contains("service-clear-std"));
		}
	}

	private static ClientPolicy policy(TranslationMissMode onMiss) {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = true;
		policy.timeout = 1000;
		policy.tendInterval = 5000;
		policy.ipMap = Map.of("127.0.0.1", "127.0.0.1");
		policy.translationOnMiss = onMiss;
		return policy;
	}

	private static Host seed(FakeNode server) {
		return new Host("127.0.0.1", server.port());
	}

	private FakeNode server(String name, String serviceStd) throws IOException {
		FakeNode server = new FakeNode(name);

		if (serviceStd != null) {
			server.serviceStd = serviceStd;
		}
		servers.add(server);
		return server;
	}

	private List<String> misses() {
		List<String> misses = new ArrayList<>();

		for (String warning : warnings()) {
			if (warning.contains("has no mapping for node")) {
				misses.add(warning);
			}
		}
		return misses;
	}

	private List<String> warnings() {
		synchronized (warnings) {
			return new ArrayList<>(warnings);
		}
	}

	private static void assertNames(String message, String... parts) {
		for (String part : parts) {
			assertTrue(message, message.contains(part));
		}
	}
}
