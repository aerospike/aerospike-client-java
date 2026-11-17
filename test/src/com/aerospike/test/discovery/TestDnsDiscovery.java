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
package com.aerospike.test.discovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.DiscoveryExecutionMode;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.DnsSeedCandidateProvider.RecordType;
import com.aerospike.client.discovery.SeedCandidateContext;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.test.util.TestEnv;

/**
 * {@link DnsSeedCandidateProvider} against the two node cluster and CoreDNS server of the
 * aerospike-client-conformance dns environment. The client gets no static seed: every seed comes from DNS. Node A is
 * reachable on 127.0.0.1 and node B on another host address, both on the same port, and each
 * advertises that address as its alternate access address. The seed list is read from the provider's debug log line, which
 * lists the hosts each refresh returns. Every case states its execution mode. The environment arrives
 * as the environment variables of its values file:
 * <ul>
 * <li>DNS_SERVER: DNS server address, ip:port</li>
 * <li>ENV_NAMESPACE: namespace, replication factor 1</li>
 * <li>NODE_A_PORT: port node A and node B are reachable on</li>
 * <li>NODE_A_NAME, NODE_A_HOST, NODE_B_NAME, NODE_B_HOST: node names and reachable addresses</li>
 * <li>NODE_C_HOST, NODE_C_PORT: address and port of the standalone node C</li>
 * <li>CLIENT_CONFIG: YAML config file selecting provider dns</li>
 * </ul>
 * The outage case stops and starts the DNS server with the stop-dns and start-dns verbs, and the
 * changed record case points _change._tcp at node C and back with set-change-srv.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDnsDiscovery {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
	private static final String SEEDS_NAME = "seeds.dns.test";
	private static final String CHANGE_NAME = "_change._tcp.dns.test";
	private static final String RESOLVED = "DNS seed names ";
	private static final int REFRESH_INTERVAL = 1000;
	private static final int WAIT_MS = 20000;
	private static final int KEY_COUNT = 100;
	private static final List<String> messages = new CopyOnWriteArrayList<>();
	private static final List<String> resolvedThreads = new CopyOnWriteArrayList<>();

	private static String server;
	private static String namespace;
	private static int port;
	private static String nodeA;
	private static String hostA;
	private static String nodeB;
	private static String hostB;
	private static String hostC;
	private static int portC;
	private static String config;

	@BeforeClass
	public static void init() {
		if (System.getenv("AEROSPIKE_CLIENT_CONFIG_URL") != null) {
			throw new IllegalStateException("AEROSPIKE_CLIENT_CONFIG_URL is set and would override the test config");
		}
		server = TestEnv.get("DNS_SERVER");
		namespace = TestEnv.get("ENV_NAMESPACE");
		port = TestEnv.getInt("NODE_A_PORT");
		nodeA = TestEnv.get("NODE_A_NAME");
		hostA = TestEnv.get("NODE_A_HOST");
		nodeB = TestEnv.get("NODE_B_NAME");
		hostB = TestEnv.get("NODE_B_HOST");
		hostC = TestEnv.get("NODE_C_HOST");
		portC = TestEnv.getInt("NODE_C_PORT");
		config = TestEnv.get("CLIENT_CONFIG");

		Log.setLevel(Log.Level.DEBUG);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN || level == Log.Level.ERROR || message.startsWith(RESOLVED)) {
				if (message.startsWith(RESOLVED)) {
					resolvedThreads.add(Thread.currentThread().getName());
				}
				messages.add(message);
				System.out.println("CLIENT-LOG " + level + ' ' + message);
			}
		});
	}

	@Before
	public void clearMessages() {
		messages.clear();
		resolvedThreads.clear();
	}

	@After
	public void clearConfig() {
		System.clearProperty(CONFIG_SYS_PROP);
	}

	@Test
	public void case1ARecordsJoinBothNodes() throws Exception {
		ClientPolicy policy = policy(DnsSeedCandidateProvider.builder().names(SEEDS_NAME).port(port).servers(server).build());

		try (AerospikeClient client = new AerospikeClient(policy)) {
			String seeds = assertJoins("a-record", client);
			assertEquals(aSeedLine(), seeds);
		}
	}

	@Test
	public void case4DnsOutageKeepsClientRunning() throws Exception {
		ClientPolicy policy = policy(DnsSeedCandidateProvider.builder().names(SEEDS_NAME).port(port).servers(server).build());

		try (AerospikeClient client = new AerospikeClient(policy)) {
			assertJoins("outage-before", client);
			messages.clear();
			control("stop-dns");

			try {
				List<String> failures = await(() -> {
					List<String> list = new ArrayList<>();

					for (String m : messages) {
						if (m.startsWith("Seed candidate refresh failed: ")) {
							list.add(m);
						}
					}
					return (list.size() >= 3)? list : null;
				});

				for (String failure : failures) {
					System.out.println("DNS-DISCOVERY-WARN " + failure.replace('\n', ' '));
					assertTrue(failure, failure.contains("DNS seed discovery failed for every name: " + SEEDS_NAME + ": "));
				}

				List<String> names = nodeNames(client);
				System.out.println("DNS-DISCOVERY-RESULT outage nodes=" + names.size() + ' ' + names +
					" refreshFailures=" + failures.size() + " connected=" + client.isConnected());
				assertEquals(Arrays.asList(nodeA, nodeB), names);
				assertTrue(messages.stream().noneMatch(m -> m.startsWith("Seed candidate provider exceeded tend deadline")));
				assertKeysWork("outage", client);
			}
			finally {
				control("start-dns");
			}
		}
	}

	@Test
	public void case5YamlARecordsJoinBothNodes() throws Exception {
		System.setProperty(CONFIG_SYS_PROP, config);

		try (AerospikeClient client = new AerospikeClient(new ClientPolicy())) {
			assertNotNull(client.getConfigProvider());
			DnsSeedCandidateProvider provider = (DnsSeedCandidateProvider)client.getClientPolicy().seedCandidateProvider;
			System.out.println("DNS-DISCOVERY-RESULT yaml provider names=" + provider.getNames() + " recordType=" +
				provider.getRecordType() + " port=" + provider.getPort() + " servers=" + provider.getServers());

			String seeds = assertJoins("yaml", client);
			assertEquals(aSeedLine(), seeds);
		}
	}

	@Test
	public void case6ThreadRefreshPicksUpChangedRecord() throws Exception {
		assertRefreshPicksUpChangedRecord(DiscoveryExecutionMode.THREAD, "discovery");
	}

	@Test
	public void case7TimeoutAboveRefreshTimeoutFailsConstruction() {
		ClientPolicy policy = policy(DnsSeedCandidateProvider.builder().names(SEEDS_NAME).port(port).servers(server)
			.timeout(300).build());
		policy.discoveryRefreshTimeout = 200;

		AerospikeException ae = assertThrows(AerospikeException.class, () -> new AerospikeClient(policy).close());
		System.out.println("DNS-DISCOVERY-RESULT timeout " + ae.getMessage() + " resolved=" + resolvedThreads.size());
		assertTrue(ae.getMessage(), ae.getMessage().contains(
			"DNS seed provider timeout 300 must be less or equal to the discovery refresh timeout 200"));
		assertEquals(0, resolvedThreads.size());
	}

	@Test
	public void case8TendRefreshPicksUpChangedRecord() throws Exception {
		assertRefreshPicksUpChangedRecord(DiscoveryExecutionMode.TEND, "tend");
	}

	private static void assertRefreshPicksUpChangedRecord(DiscoveryExecutionMode mode, String thread) throws Exception {
		ClientPolicy policy = policy(DnsSeedCandidateProvider.builder().names(CHANGE_NAME).recordType(RecordType.SRV)
			.servers(server).build());
		policy.discoveryExecutionMode = mode;
		Host nodeC = new Host(hostC, portC);

		try (AerospikeClient client = new AerospikeClient(policy)) {
			assertJoins("change-before", client);
			assertFalse(Arrays.asList(ClusterInternals.seeds(client)).contains(nodeC));
			int from = resolvedThreads.size();
			control("set-change-srv", "node-c");

			try {
				String changed = await(() -> messages.stream().filter(m -> m.startsWith(RESOLVED) &&
					m.contains(hostC + " " + portC)).findFirst().orElse(null));
				await(() -> Arrays.asList(ClusterInternals.seeds(client)).contains(nodeC)? Boolean.TRUE : null);
				Set<String> threads = new HashSet<>(resolvedThreads.subList(from, resolvedThreads.size()));
				List<String> names = nodeNames(client);
				System.out.println("DNS-DISCOVERY-RESULT change mode=" + mode + " '" + changed + "' seeds=" +
					Arrays.asList(ClusterInternals.seeds(client)) + " resolvedOn=" + threads + " nodes=" + names);
				assertEquals(new HashSet<>(Arrays.asList(thread)), threads);
				assertEquals(Arrays.asList(nodeA, nodeB), names);
				assertKeysWork("change", client);
			}
			finally {
				control("set-change-srv", "node-a", "node-b");
				DnsSeedCandidateProvider check = DnsSeedCandidateProvider.builder().names(CHANGE_NAME)
					.recordType(RecordType.SRV).servers(server).build();
				List<Host> restored = Arrays.asList(new Host(hostA, port), new Host(hostB, port));
				await(() -> new HashSet<>(check.refreshSeedCandidates(new SeedCandidateContext(null, new Host[0], 0)))
					.equals(new HashSet<>(restored))? Boolean.TRUE : null);
			}
		}
	}

	private static ClientPolicy policy(DnsSeedCandidateProvider provider) {
		ClientPolicy policy = new ClientPolicy();
		policy.discoveryExecutionMode = DiscoveryExecutionMode.THREAD;
		policy.useServicesAlternate = true;
		policy.discoveryRefreshInterval = REFRESH_INTERVAL;
		policy.seedCandidateProvider = provider;
		return policy;
	}

	private static String assertJoins(String name, AerospikeClient client) throws Exception {
		String seeds = messages.stream().filter(m -> m.startsWith(RESOLVED)).findFirst().orElse(null);
		assertNotNull("no seed list logged: " + messages, seeds);

		await(() -> (client.getNodes().length == 2 && ownedPartitions(client) == Node.PARTITIONS)? Boolean.TRUE : null);
		List<String> names = nodeNames(client);
		int owned = ownedPartitions(client);

		System.out.println("DNS-DISCOVERY-RESULT " + name + " seeds='" + seeds + "' nodes=" + names.size() + ' ' + names +
			" ownedPartitions=" + owned + '/' + Node.PARTITIONS);
		assertEquals(Arrays.asList(nodeA, nodeB), names);
		assertEquals(Node.PARTITIONS, owned);
		assertKeysWork(name, client);
		return seeds;
	}

	private static void assertKeysWork(String name, AerospikeClient client) {
		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(namespace, "dns-discovery", i);
			client.put(null, key, new Bin("v", i));
			Record record = client.get(null, key);
			assertEquals(i, record.getInt("v"));
		}
		System.out.println("DNS-DISCOVERY-RESULT " + name + " keys=" + KEY_COUNT + " written and read");
	}

	private static int ownedPartitions(AerospikeClient client) {
		Partitions partitions = client.getCluster().partitionMap.get(namespace);

		if (partitions == null) {
			return 0;
		}

		int owned = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = partitions.replicas[0].get(i);

			if (node != null && node.isActive()) {
				owned++;
			}
		}
		return owned;
	}

	private static List<String> nodeNames(AerospikeClient client) {
		List<String> names = new ArrayList<>();

		for (Node node : client.getNodes()) {
			names.add(node.getName());
		}
		names.sort(null);
		return names;
	}

	private static String aSeedLine() {
		return (hostA.compareTo(hostB) < 0)? seedLine(SEEDS_NAME, hostA, port, hostB, port) :
			seedLine(SEEDS_NAME, hostB, port, hostA, port);
	}

	private static String seedLine(String name, Object... hostPorts) {
		List<String> hosts = new ArrayList<>();

		for (int i = 0; i < hostPorts.length; i += 2) {
			hosts.add(hostPorts[i] + " " + hostPorts[i + 1]);
		}
		return RESOLVED + Arrays.asList(name) + " resolved to " + hosts;
	}

	private static void control(String... verb) throws Exception {
		String output = TestEnv.control(verb);
		System.out.println("DNS-DISCOVERY-CONTROL " + Arrays.asList(verb) + ": " + output);
	}

	private static <T> T await(Supplier<T> condition) throws Exception {
		long deadline = System.currentTimeMillis() + WAIT_MS;
		T value;

		while ((value = condition.get()) == null) {
			if (System.currentTimeMillis() > deadline) {
				fail("condition not met within " + WAIT_MS + "ms: " + messages);
			}
			Thread.sleep(50);
		}
		return value;
	}
}
