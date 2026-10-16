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
import java.util.List;
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
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Translated address collisions against the two node clusters started by
 * test/translation/run_collision.sh. In the control cluster each node advertises 127.0.0.1 and its
 * own published host port in its alternate access address. In the collision cluster node B
 * advertises node A's published port. Every case seeds node A only. The clusters arrive as system
 * properties:
 * <ul>
 * <li>translation.host: host the nodes are published on</li>
 * <li>translation.namespace: namespace, replication factor 1</li>
 * <li>control.nodeA, control.portA, control.nodeB, control.portB: control cluster</li>
 * <li>collision.nodeA, collision.portA, collision.nodeB: collision cluster</li>
 * </ul>
 */
public class TestTranslationCollision {
	private static final int TEND_INTERVAL = 250;
	private static final int COLLISION_TENDS = 8;
	private static final int JOIN_WAIT_MS = 10000;
	private static final int KEY_COUNT = 200;
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	private static String host;
	private static String namespace;
	private static String controlA;
	private static int controlPortA;
	private static String controlB;
	private static int controlPortB;
	private static String collisionA;
	private static int collisionPortA;
	private static String collisionB;

	public static final class SingleAddressTranslator implements AddressTranslator {
		private final Host target;

		public SingleAddressTranslator(Host target) {
			this.target = target;
		}

		@Override
		public Optional<Host> translate(Endpoint advertised) {
			return Optional.of(target);
		}
	}

	@BeforeClass
	public static void init() {
		host = required("translation.host");
		namespace = required("translation.namespace");
		controlA = required("control.nodeA");
		controlPortA = Integer.parseInt(required("control.portA"));
		controlB = required("control.nodeB");
		controlPortB = Integer.parseInt(required("control.portB"));
		collisionA = required("collision.nodeA");
		collisionPortA = Integer.parseInt(required("collision.portA"));
		collisionB = required("collision.nodeB");

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
	public void distinctPortsJoinBothNodes() throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy(), new Host(host, controlPortA))) {
			long deadline = System.currentTimeMillis() + JOIN_WAIT_MS;

			while (client.getNodes().length < 2 && System.currentTimeMillis() < deadline) {
				Thread.sleep(TEND_INTERVAL);
			}

			List<String> names = nodeNames(client);
			System.out.println("COLLISION-RESULT control nodes=" + names.size() + ' ' + names +
				" collisionWarnings=" + collisionWarnings().size());
			assertEquals(List.of(controlA, controlB), names);
			assertEquals(List.of(), collisionWarnings());
			assertEquals(controlPortB, client.getCluster().getNode(controlB).getHost().port);
		}
	}

	@Test
	public void sharedAlternatePortCreatesNoPeer() throws Exception {
		assertCollides("collision", policy(), collisionPortA, collisionA, collisionB, "static-map");
	}

	@Test
	public void customTranslatorToNodeACreatesNoPeer() throws Exception {
		ClientPolicy policy = policy();
		policy.addressTranslator = new SingleAddressTranslator(new Host(host, controlPortA));
		assertCollides("custom", policy, controlPortA, controlA, controlB, SingleAddressTranslator.class.getName());
	}

	private static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.useServicesAlternate = true;
		policy.tendInterval = TEND_INTERVAL;
		return policy;
	}

	private static void assertCollides(String name, ClientPolicy policy, int portA, String nodeA, String nodeB,
		String translator) throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA))) {
			for (int i = 0; i < COLLISION_TENDS; i++) {
				Thread.sleep(TEND_INTERVAL);
				assertEquals(List.of(nodeA), nodeNames(client));
			}

			List<String> collisions = collisionWarnings();
			System.out.println("COLLISION-RESULT " + name + " nodes=" + client.getNodes().length + ' ' +
				nodeNames(client) + " collisionWarnings=" + collisions.size());

			if (collisions.isEmpty()) {
				fail("No translation collision logged for node " + nodeB + ": " + warnings);
			}

			String message = collisions.get(0);
			System.out.println("COLLISION-LOG " + name + ' ' + message);

			for (String part : new String[] {nodeA, nodeB, host + ':' + portA, translator, "Give each node its own reachable address or mapping"}) {
				assertTrue(message, message.contains(part));
			}
			assertTrue("Colliding peer not retried on a later tend: " + collisions, collisions.size() >= 2);
			assertOwnedKeysWork(name, client, nodeA);
		}
	}

	private static void assertOwnedKeysWork(String name, AerospikeClient client, String nodeA) {
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
		System.out.println("COLLISION-KEYS " + name + " ownedByA=" + ownedByA + " notOwned=" + notOwned);
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

	private static List<String> collisionWarnings() {
		List<String> collisions = new ArrayList<>();

		for (String warning : warnings) {
			if (warning.contains("to the same address")) {
				collisions.add(warning);
			}
		}
		return collisions;
	}

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/translation/run_collision.sh");
		}
		return value;
	}
}
