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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.DnsSeedCandidateProvider.RecordType;
import com.aerospike.client.policy.ClientPolicy;

/**
 * {@link DnsSeedCandidateProvider} against the two node cluster and CoreDNS server started by
 * test/discovery/run_dns.sh. The client gets no static seed: every seed comes from DNS. Node A is
 * reachable on 127.0.0.1 and node B on another host address, both on the same port, and each
 * advertises that address as its alternate access address. A standalone node C is published on
 * 127.0.0.1 on another port. The seed list is read from the provider's debug log line, which
 * lists the hosts each refresh returns. The environment arrives as system properties:
 * <ul>
 * <li>dns.server: DNS server address, ip:port</li>
 * <li>dns.zoneFile: zone file served by the DNS server, rewritten by the record change case</li>
 * <li>dns.container: DNS server container, stopped by the outage case</li>
 * <li>dns.namespace: namespace, replication factor 1</li>
 * <li>dns.port: port node A and node B are reachable on</li>
 * <li>dns.nodeA, dns.hostA, dns.nodeB, dns.hostB: node names and reachable addresses</li>
 * <li>dns.hostC, dns.portC: node C published address and port</li>
 * <li>dns.config: YAML config file selecting provider dns</li>
 * </ul>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDnsDiscovery {
	private static final String CONFIG_SYS_PROP = "AEROSPIKE_CLIENT_CONFIG_SYS_PROP";
	private static final String SEEDS_NAME = "seeds.dns.test";
	private static final String SRV_NAME = "_aerospike._tcp.dns.test";
	private static final String CHANGE_NAME = "_change._tcp.dns.test";
	private static final String RESOLVED = "DNS seed names ";
	private static final int REFRESH_INTERVAL = 1000;
	private static final int WAIT_MS = 20000;
	private static final int KEY_COUNT = 100;
	private static final Pattern SERIAL = Pattern.compile("(SOA\\s+\\S+\\s+\\S+\\s+)(\\d+)");
	private static final List<String> messages = new CopyOnWriteArrayList<>();

	private static String server;
	private static Path zoneFile;
	private static String container;
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
		server = required("dns.server");
		zoneFile = Paths.get(required("dns.zoneFile"));
		container = required("dns.container");
		namespace = required("dns.namespace");
		port = Integer.parseInt(required("dns.port"));
		nodeA = required("dns.nodeA");
		hostA = required("dns.hostA");
		nodeB = required("dns.nodeB");
		hostB = required("dns.hostB");
		hostC = required("dns.hostC");
		portC = Integer.parseInt(required("dns.portC"));
		config = required("dns.config");

		Log.setLevel(Log.Level.DEBUG);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN || level == Log.Level.ERROR || message.startsWith(RESOLVED)) {
				messages.add(message);
				System.out.println("CLIENT-LOG " + level + ' ' + message);
			}
		});
	}

	@Before
	public void clearMessages() {
		messages.clear();
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
	public void case2SrvRecordsJoinBothNodesWithIpLiteralSeeds() throws Exception {
		ClientPolicy policy = policy(DnsSeedCandidateProvider.builder().names(SRV_NAME).recordType(RecordType.SRV)
			.servers(server).build());

		try (AerospikeClient client = new AerospikeClient(policy)) {
			String seeds = assertJoins("srv", client);
			assertEquals(seedLine(SRV_NAME, hostA, port, hostB, port), seeds);
			assertFalse(seeds, seeds.contains("dns.test ") || seeds.contains("dns.test. "));
		}
	}

	@Test
	public void case3RecordChangeReachesSeedList() throws Exception {
		ClientPolicy policy = policy(DnsSeedCandidateProvider.builder().names(CHANGE_NAME).recordType(RecordType.SRV)
			.servers(server).build());
		String original = new String(Files.readAllBytes(zoneFile), StandardCharsets.UTF_8);

		try (AerospikeClient client = new AerospikeClient(policy)) {
			String before = assertJoins("change-before", client);
			assertEquals(seedLine(CHANGE_NAME, hostA, port, hostB, port), before);

			messages.clear();
			writeZone(changeRecord(original));
			long begin = System.nanoTime();
			String expected = seedLine(CHANGE_NAME, hostC, portC);
			String after = await(() -> messages.stream().filter(expected::equals).findFirst().orElse(null));
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

			System.out.println("DNS-DISCOVERY-RESULT change seedsAfter='" + after + "' elapsedMs=" + elapsed +
				" refreshIntervalMs=" + REFRESH_INTERVAL + " nodes=" + nodeNames(client));
			assertEquals(List.of(nodeA, nodeB), nodeNames(client));
		}
		finally {
			writeZone(original);
		}
	}

	@Test
	public void case4DnsOutageKeepsClientRunning() throws Exception {
		ClientPolicy policy = policy(DnsSeedCandidateProvider.builder().names(SEEDS_NAME).port(port).servers(server).build());

		try (AerospikeClient client = new AerospikeClient(policy)) {
			assertJoins("outage-before", client);
			messages.clear();
			docker("stop", "-t", "1", container);

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
				assertEquals(List.of(nodeA, nodeB), names);
				assertTrue(messages.stream().noneMatch(m -> m.startsWith("Seed candidate provider exceeded tend deadline")));
				assertKeysWork("outage", client);
			}
			finally {
				docker("start", container);
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

	private static ClientPolicy policy(DnsSeedCandidateProvider provider) {
		ClientPolicy policy = new ClientPolicy();
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
		assertEquals(List.of(nodeA, nodeB), names);
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
		return RESOLVED + List.of(name) + " resolved to " + hosts;
	}

	private static String changeRecord(String zone) {
		StringBuilder sb = new StringBuilder();
		boolean replaced = false;

		for (String line : zone.split("\n", -1)) {
			if (line.startsWith("_change._tcp ")) {
				if (! replaced) {
					sb.append("_change._tcp 1 IN SRV 10 10 ").append(portC).append(" node-c\n");
					replaced = true;
				}
				continue;
			}
			sb.append(line).append('\n');
		}
		sb.setLength(sb.length() - 1);
		assertTrue("no _change._tcp record in " + zoneFile, replaced);
		return sb.toString();
	}

	private static void writeZone(String zone) throws Exception {
		String current = new String(Files.readAllBytes(zoneFile), StandardCharsets.UTF_8);
		Matcher m = SERIAL.matcher(current);
		assertTrue("no SOA serial in " + zoneFile, m.find());
		long serial = Long.parseLong(m.group(2)) + 1;
		String content = SERIAL.matcher(zone).replaceFirst("$1" + serial);
		Path tmp = zoneFile.resolveSibling(zoneFile.getFileName() + ".tmp");
		Files.write(tmp, content.getBytes(StandardCharsets.UTF_8));
		Files.move(tmp, zoneFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
	}

	private static void docker(String... args) throws Exception {
		List<String> command = new ArrayList<>();
		command.add("docker");
		command.addAll(Arrays.asList(args));
		Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();

		if (! process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
			fail(command + " failed: " + output);
		}
		System.out.println("DNS-DISCOVERY-DOCKER " + command + ": " + output);
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

	private static String required(String name) {
		String value = System.getProperty(name);

		if (value == null || value.isEmpty()) {
			throw new IllegalStateException("System property " + name + " is not set. Run test/discovery/run_dns.sh");
		}
		return value;
	}
}
