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
package com.aerospike.test.translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.discovery.AddressTranslator;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * {@link ClientPolicy#translationOnMiss} against a two node cluster started by
 * test/translation/run_on_miss.sh. Each node advertises 127.0.0.1 and its own published host port
 * in its alternate access address, so node B is reachable only as advertised. Every case seeds
 * node A only. The cluster arrives as system properties:
 * <ul>
 * <li>translation.host: host both nodes are published on</li>
 * <li>translation.nodeA, translation.portA: seed node name and port</li>
 * <li>translation.nodeB, translation.portB: peer node name and port</li>
 * <li>translation.namespace: namespace, replication factor 1</li>
 * </ul>
 */
public class TestTranslationOnMiss {
	private static final int TEND_INTERVAL = 250;
	private static final int MISS_TENDS = 8;
	private static final int JOIN_WAIT_MS = 10000;
	private static final int KEY_COUNT = 200;
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	private static String host;
	private static String nodeA;
	private static int portA;
	private static String nodeB;
	private static int portB;
	private static String namespace;

	public static final class EmptyTranslator implements AddressTranslator {
		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.empty();
		}
	}

	@BeforeClass
	public static void init() {
		host = required("translation.host");
		nodeA = required("translation.nodeA");
		portA = Integer.parseInt(required("translation.portA"));
		nodeB = required("translation.nodeB");
		portB = Integer.parseInt(required("translation.portB"));
		namespace = required("translation.namespace");

		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
			System.out.println("CLIENT-LOG " + level + ' ' + message);
		});
	}

	@Before
	public void clearWarnings() {
		warnings.clear();
	}

	@Test
	public void defaultJoinsBothNodes() throws Exception {
		assertJoins("default", policy());
	}

	@Test
	public void emptyIpMapStrictDropsPeer() throws Exception {
		ClientPolicy policy = policy();
		policy.ipMap = new HashMap<>();
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertMisses("ipmap-empty-strict", policy, "static-map");
	}

	@Test
	public void identityIpMapStrictJoinsBothNodes() throws Exception {
		ClientPolicy policy = policy();
		policy.ipMap = Map.of(host, host);
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertJoins("ipmap-identity-strict", policy);
	}

	@Test
	public void customEmptyStrictDropsPeer() throws Exception {
		ClientPolicy policy = policy();
		policy.addressTranslator = new EmptyTranslator();
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertMisses("custom-empty-strict", policy, EmptyTranslator.class.getName());
	}

	@Test
	public void customEmptyPassThroughJoinsBothNodes() throws Exception {
		ClientPolicy policy = policy();
		policy.addressTranslator = new EmptyTranslator();
		policy.translationOnMiss = TranslationMissMode.PASS_THROUGH;
		assertJoins("custom-empty-pass-through", policy);
	}

	private static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.useServicesAlternate = true;
		policy.tendInterval = TEND_INTERVAL;
		return policy;
	}

	private static void assertJoins(String name, ClientPolicy policy) throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA))) {
			long deadline = System.currentTimeMillis() + JOIN_WAIT_MS;

			while (client.getNodes().length < 2 && System.currentTimeMillis() < deadline) {
				Thread.sleep(TEND_INTERVAL);
			}

			List<String> names = nodeNames(client);
			System.out.println("ON-MISS-RESULT " + name + " nodes=" + names.size() + ' ' + names);
			assertEquals(List.of(nodeA, nodeB), names);
			assertEquals(List.of(), missWarnings());
		}
	}

	private static void assertMisses(String name, ClientPolicy policy, String translator) throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA))) {
			for (int i = 0; i < MISS_TENDS; i++) {
				Thread.sleep(TEND_INTERVAL);
				assertEquals(List.of(nodeA), nodeNames(client));
			}

			List<String> misses = missWarnings();
			System.out.println("ON-MISS-RESULT " + name + " nodes=" + client.getNodes().length + ' ' +
				nodeNames(client) + " missWarnings=" + misses.size());

			if (misses.isEmpty()) {
				fail("No translation miss logged for node " + nodeB + ": " + warnings);
			}

			String message = misses.get(0);
			System.out.println("ON-MISS-LOG " + name + ' ' + message);

			for (String part : new String[] {nodeB, host + ':' + portB, "alternate", translator, "pass_through"}) {
				assertTrue(message, message.contains(part));
			}
			assertTrue("Missed peer not retried on a later tend: " + misses, misses.size() >= 2);
			assertOwnedKeysWork(name, client);
		}
	}

	private static void assertOwnedKeysWork(String name, AerospikeClient client) {
		Cluster cluster = client.getCluster();
		WritePolicy policy = client.copyWritePolicyDefault();
		int ownedByA = 0;
		int notOwned = 0;

		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(namespace, "translation", i);
			Node node;

			try {
				node = Partition.write(cluster, policy, key).getNodeWrite(cluster);
			}
			catch (AerospikeException.InvalidNode ine) {
				notOwned++;
				continue;
			}

			assertEquals(nodeA, node.getName());
			client.put(policy, key, new Bin("v", i));
			Record record = client.get(null, key);
			assertEquals(i, record.getInt("v"));
			ownedByA++;
		}
		System.out.println("ON-MISS-KEYS " + name + " ownedByA=" + ownedByA + " notOwned=" + notOwned);
		assertTrue(ownedByA > 0);
		assertTrue(notOwned > 0);
	}

	private static List<String> nodeNames(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		names.sort(null);
		return names;
	}

	private static List<String> missWarnings() {
		List<String> misses = new ArrayList<>();

		for (String warning : warnings) {
			if (warning.contains("has no mapping for node " + nodeB)) {
				misses.add(warning);
			}
		}
		return misses;
	}

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/translation/run_on_miss.sh");
		}
		return value;
	}
}
