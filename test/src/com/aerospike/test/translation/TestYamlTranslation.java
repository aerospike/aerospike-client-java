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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.After;
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
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * Address translation configured only through the YAML config file, against two clusters started
 * by test/translation/run_yaml.sh. Each case constructs the client with a default
 * {@link ClientPolicy} and seeds node A only.
 * <ul>
 * <li>pass-through: each node advertises 127.0.0.1 and its own published host port in its
 * alternate access address. The YAML sets use_services_alternate and translation.provider
 * pass-through.</li>
 * <li>port-map: each node advertises only its container address. The YAML sets
 * translation.provider port-map with a port_map to the published host ports.</li>
 * </ul>
 * The clusters arrive as system properties:
 * <ul>
 * <li>yaml.host: host every node is published on</li>
 * <li>yaml.namespace: namespace, replication factor 1</li>
 * <li>yaml.&lt;case&gt;.config: YAML config file</li>
 * <li>yaml.&lt;case&gt;.nodeA, yaml.&lt;case&gt;.portA: seed node name and port</li>
 * <li>yaml.&lt;case&gt;.nodeB, yaml.&lt;case&gt;.portB: peer node name and port</li>
 * </ul>
 */
public class TestYamlTranslation {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
	private static final int TEND_INTERVAL = 250;
	private static final int JOIN_WAIT_MS = 10000;
	private static final int KEY_COUNT = 200;
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	private static String host;
	private static String namespace;

	@BeforeClass
	public static void init() {
		if (System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") != null) {
			throw new IllegalStateException("AEROSPIKE_CLIENT_CONFIG_URL is set and would override the test config");
		}
		host = required("yaml.host");
		namespace = required("yaml.namespace");

		Log.setLevel(Log.Level.INFO);
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

	@After
	public void clearConfig() {
		System.clearProperty(CONFIG_SYS_PROP);
	}

	@Test
	public void passThroughFromYaml() throws Exception {
		assertJoins("passThrough", policy -> {
			assertTrue(policy.useServicesAlternate);
			assertSame(PassThroughAddressTranslator.INSTANCE, policy.addressTranslator);
			assertEquals(TranslationMissMode.PASS_THROUGH, policy.translationOnMiss);
		});
	}

	@Test
	public void portMapFromYaml() throws Exception {
		assertJoins("portMap", policy -> {
			assertFalse(policy.useServicesAlternate);
			assertTrue(policy.addressTranslator instanceof PortMapAddressTranslator);
			assertEquals(TranslationMissMode.STRICT, policy.translationOnMiss);
		});
	}

	private interface PolicyCheck {
		void check(ClientPolicy policy);
	}

	private static void assertJoins(String name, PolicyCheck check) throws Exception {
		String config = required("yaml." + name + ".config");
		String nodeA = required("yaml." + name + ".nodeA");
		int portA = Integer.parseInt(required("yaml." + name + ".portA"));
		String nodeB = required("yaml." + name + ".nodeB");
		int portB = Integer.parseInt(required("yaml." + name + ".portB"));

		System.setProperty(CONFIG_SYS_PROP, config);

		ClientPolicy codePolicy = new ClientPolicy();
		assertNull(codePolicy.addressTranslator);

		try (AerospikeClient client = new AerospikeClient(codePolicy, new Host(host, portA))) {
			ClientPolicy policy = client.getClientPolicy();
			assertEquals(TEND_INTERVAL, policy.tendInterval);
			check.check(policy);

			long deadline = System.currentTimeMillis() + JOIN_WAIT_MS;

			while ((client.getNodes().length < 2 || unowned(client) > 0) && System.currentTimeMillis() < deadline) {
				Thread.sleep(TEND_INTERVAL);
			}

			Map<String,String> hosts = new TreeMap<>();

			for (Node node : client.getNodes()) {
				hosts.put(node.getName(), node.getHost().name + ':' + node.getHost().port);
			}
			System.out.println("YAML-RESULT " + name + " translator=" + policy.addressTranslator.getClass().getSimpleName() +
				" useServicesAlternate=" + policy.useServicesAlternate + " onMiss=" + policy.translationOnMiss +
				" nodes=" + hosts.size() + ' ' + hosts);
			assertEquals(Map.of(nodeA, host + ':' + portA, nodeB, host + ':' + portB), hosts);

			Map<String,Integer> owners = owners(client);
			int unowned = unowned(client);
			System.out.println("YAML-PARTITIONS " + name + " partitions=" + Node.PARTITIONS +
				" unowned=" + unowned + " owners=" + owners);
			assertEquals(0, unowned);
			assertEquals(2, owners.size());
			assertTrue(owners.get(nodeA) > 0);
			assertTrue(owners.get(nodeB) > 0);

			assertKeysWork(name, client, nodeA);
			System.out.println("YAML-WARNINGS " + name + ' ' + warnings);
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

	private static void assertKeysWork(String name, AerospikeClient client, String nodeA) {
		WritePolicy policy = client.copyWritePolicyDefault();
		int onA = 0;
		int onB = 0;

		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(namespace, "yaml-translation", i);
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
		System.out.println("YAML-KEYS " + name + " onA=" + onA + " onB=" + onB);
		assertTrue(onA > 0);
		assertTrue(onB > 0);
	}

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/translation/run_yaml.sh");
		}
		return value;
	}
}
