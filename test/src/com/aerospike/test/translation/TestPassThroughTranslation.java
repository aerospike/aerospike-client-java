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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.PassThroughAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * {@link PassThroughAddressTranslator} against a two node cluster started by
 * test/translation/run_pass_through.sh. Each node advertises 127.0.0.1 and its own published host
 * port in its alternate access address: one host name, a port per node. Every case seeds node A
 * only. The cluster arrives as system properties:
 * <ul>
 * <li>translation.host: host both nodes are published on</li>
 * <li>translation.nodeA, translation.portA: seed node name and port</li>
 * <li>translation.nodeB, translation.portB: peer node name and port</li>
 * <li>translation.namespace: namespace, replication factor 1</li>
 * </ul>
 */
public class TestPassThroughTranslation {
	private static final int TEND_INTERVAL = 250;
	private static final int JOIN_WAIT_MS = 10000;
	private static final int KEY_COUNT = 200;
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	private static String host;
	private static String nodeA;
	private static int portA;
	private static String nodeB;
	private static int portB;
	private static String namespace;

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
	public void defaultOnMissJoinsBothNodes() throws Exception {
		assertJoins("default", policy());
	}

	@Test
	public void strictJoinsBothNodes() throws Exception {
		ClientPolicy policy = policy();
		policy.translationOnMiss = TranslationMissMode.STRICT;
		assertJoins("strict", policy);
	}

	private static ClientPolicy policy() {
		ClientPolicy policy = new ClientPolicy();
		policy.useServicesAlternate = true;
		policy.addressTranslator = PassThroughAddressTranslator.INSTANCE;
		policy.tendInterval = TEND_INTERVAL;
		return policy;
	}

	private static void assertJoins(String name, ClientPolicy policy) throws Exception {
		try (AerospikeClient client = new AerospikeClient(policy, new Host(host, portA))) {
			long deadline = System.currentTimeMillis() + JOIN_WAIT_MS;

			while ((client.getNodes().length < 2 || unowned(client) > 0) && System.currentTimeMillis() < deadline) {
				Thread.sleep(TEND_INTERVAL);
			}

			Map<String,String> hosts = new TreeMap<>();

			for (Node node : client.getNodes()) {
				hosts.put(node.getName(), node.getHost().name + ':' + node.getHost().port);
			}
			System.out.println("PASS-THROUGH-RESULT " + name + " nodes=" + hosts.size() + ' ' + hosts);
			assertEquals(Map.of(nodeA, host + ':' + portA, nodeB, host + ':' + portB), hosts);

			Map<String,Integer> owners = owners(client);
			int unowned = unowned(client);
			System.out.println("PASS-THROUGH-PARTITIONS " + name + " partitions=" + Node.PARTITIONS +
				" unowned=" + unowned + " owners=" + owners);
			assertEquals(0, unowned);
			assertEquals(2, owners.size());
			assertTrue(owners.get(nodeA) > 0);
			assertTrue(owners.get(nodeB) > 0);

			assertKeysWork(name, client);
			System.out.println("PASS-THROUGH-WARNINGS " + name + ' ' + warnings);
			assertEquals(List.of(), warnings);
		}
	}

	private static int unowned(AerospikeClient client) {
		Partitions partitions = client.getCluster().partitionMap.get(namespace);

		if (partitions == null) {
			return Node.PARTITIONS;
		}

		AtomicReferenceArray<Node> master = partitions.replicas[0];
		int unowned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = master.get(i);

			if (node == null || ! node.isActive()) {
				unowned++;
			}
		}
		return unowned;
	}

	private static Map<String,Integer> owners(AerospikeClient client) {
		AtomicReferenceArray<Node> master = client.getCluster().partitionMap.get(namespace).replicas[0];
		Map<String,Integer> owners = new TreeMap<>();

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = master.get(i);

			if (node != null) {
				owners.merge(node.getName(), 1, Integer::sum);
			}
		}
		return owners;
	}

	private static void assertKeysWork(String name, AerospikeClient client) {
		WritePolicy policy = client.copyWritePolicyDefault();
		int onA = 0;
		int onB = 0;

		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(namespace, "pass-through", i);
			Node node = Partition.write(client.getCluster(), policy, key).getNodeWrite(client.getCluster());

			client.put(policy, key, new Bin("v", i));
			Record record = client.get(null, key);
			assertEquals(i, record.getInt("v"));

			if (node.getName().equals(nodeA)) {
				onA++;
			}
			else {
				onB++;
			}
		}
		System.out.println("PASS-THROUGH-KEYS " + name + " onA=" + onA + " onB=" + onB);
		assertTrue(onA > 0);
		assertTrue(onB > 0);
	}

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/translation/run_pass_through.sh");
		}
		return value;
	}
}
