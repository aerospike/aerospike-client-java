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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
import com.aerospike.client.discovery.DnsSeedCandidateProvider.RecordType;
import com.aerospike.client.discovery.Endpoint;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.discovery.L2Topology.L2Node;
import com.aerospike.test.discovery.L2Topology.Peer;
import com.aerospike.test.util.TestBase;

/**
 * Testing strategy section 4.3 rows N-1 to N-8 against the {@link L2Topology}. N-3 is retired.
 * Every case seeds the first listener and builds its own {@link ClientPolicy}.
 */
public class TestL2Topology extends TestBase {
	private static final String SET = "l2";
	private static final int KEY_COUNT = 300;
	private static final int MISS_RETRIES = 3;
	private static final String DNS_BLOCKER = "N-4 needs a DNS server on the L2 client network serving SRV " +
		"records for the endpoint; the L2 topology has none yet (E4 gap)";
	private static final List<String> warnings = new CopyOnWriteArrayList<>();

	private String disabledProxy;

	@After
	public void restore() throws Exception {
		Log.setCallback(null);

		if (disabledProxy != null) {
			L2Topology.setListener(disabledProxy, true);
			disabledProxy = null;
		}
	}

	/**
	 * N-1: baseline, no translator. The client reaches the seed through the endpoint and then
	 * fails on the peers, whose advertised addresses it has no route to.
	 */
	@Test
	public void n1BaselineWithoutTranslatorFailsOnPeers() {
		Host seed = new Host(L2Topology.host, L2Topology.ports.get(0));
		Map<String,String> info = null;

		try {
			info = Info.request(seed.name, seed.port, "node", "peers-clear-std");
		}
		catch (AerospikeException ae) {
			fail("N-1 guard: seed " + seed + " is not reachable through the endpoint: " + ae.getMessage());
		}

		String node = info.get("node");
		String reply = info.get("peers-clear-std");
		List<Peer> peers = L2Topology.parsePeers(reply);
		System.out.println("L2-N1-GUARD seed=" + seed + " node=" + node + " peers-clear-std=" + reply);
		assertTrue("N-1 guard: seed " + seed + " returned no node name", node != null && ! node.isEmpty());
		assertFalse("N-1 guard: seed " + node + " lists no peers: " + reply, peers.isEmpty());

		for (Peer peer : peers) {
			for (String endpoint : peer.endpoints) {
				L2Topology.assertUnreachable(endpoint);
			}
		}

		ClientPolicy policy = L2Topology.policy();

		try (AerospikeClient client = new AerospikeClient(policy, seed)) {
			fail("N-1: client joined " + L2Topology.nodeNames(client) + " without a translator");
		}
		catch (AerospikeException ae) {
			String message = ae.getMessage();
			System.out.println("L2-N1-ERROR " + message);
			assertTrue(message, message.contains("Peers not reachable: "));

			for (Peer peer : peers) {
				for (String endpoint : peer.endpoints) {
					int index = endpoint.lastIndexOf(':');
					String unreachable = endpoint.substring(0, index) + ' ' + endpoint.substring(index + 1);
					assertTrue(unreachable + " not named in: " + message, message.contains(unreachable));
				}
			}
		}
	}

	/**
	 * N-2: port-map keyed by advertised {@code (host, port)}. All nodes join and the partition
	 * map is complete.
	 */
	@Test
	public void n2PortMapByHostPortJoinsAllNodes() throws Exception {
		assertPortMapJoins("N-2-host-port", L2Topology.portMapByEndpoint(L2Topology.nodes()));
	}

	/**
	 * N-2: port-map keyed by node name. All nodes join and the partition map is complete.
	 */
	@Test
	public void n2PortMapByNodeNameJoinsAllNodes() throws Exception {
		assertPortMapJoins("N-2-node-name", L2Topology.portMapByNodeName(L2Topology.nodes()));
	}

	/**
	 * N-4: dns provider end to end. The seed comes from SRV records for the endpoint, one per
	 * listener, and the peers through port-map.
	 */
	@Test
	public void n4DnsProviderSeedsThroughEndpoint() throws Exception {
		skipUnless(false, DNS_BLOCKER);

		ClientPolicy policy = L2Topology.policy();
		policy.seedCandidateProvider = DnsSeedCandidateProvider.builder()
			.names("_aerospike._tcp." + L2Topology.host)
			.recordType(RecordType.SRV)
			.build();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(L2Topology.nodes()));

		try (AerospikeClient client = new AerospikeClient(policy)) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			L2Topology.assertAllPartitionsOwned("N-4", client);
		}
	}

	/**
	 * N-5: no SeedOnlyCluster. Every node owns partitions as its own {@link Node}, and writes and
	 * reads for keys spread across partitions are served by the owner without a server proxy hop.
	 */
	@Test
	public void n5EveryPartitionOwnerIsADistinctNode() throws Exception {
		List<L2Node> nodes = L2Topology.nodes();
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(nodes));

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			Node[] masters = L2Topology.assertAllPartitionsOwned("N-5", client);
			Set<Node> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
			Collections.addAll(distinct, masters);
			Map<String,Integer> owners = L2Topology.owners(masters);
			System.out.println("L2-N5-OWNERS nodes=" + distinct.size() + " partitionsPerNode=" + owners);
			assertEquals(nodes.size(), distinct.size());
			assertEquals(L2Topology.nodeNames(), new ArrayList<>(owners.keySet()));

			for (Node node : client.getNodes()) {
				assertSame(node.getName(), node, client.getCluster().getNode(node.getName()));
				assertTrue(node.getName(), distinct.contains(node));
			}

			String[] stats = {"client_write_success", "client_read_success", "client_proxy_complete",
				"client_proxy_error", "client_proxy_timeout"};
			Map<String,Map<String,Long>> before = new TreeMap<>();

			for (L2Node node : nodes) {
				before.put(node.name, L2Topology.namespaceStats(node, stats));
			}

			Cluster cluster = client.getCluster();
			WritePolicy wp = client.copyWritePolicyDefault();
			wp.replica = Replica.MASTER;
			Policy rp = client.copyReadPolicyDefault();
			rp.replica = Replica.MASTER;
			Map<String,Long> served = new TreeMap<>();
			Set<Integer> partitions = new HashSet<>();

			for (int i = 0; i < KEY_COUNT; i++) {
				Key key = new Key(L2Topology.namespace, SET, "n5-" + i);
				Node owner = masters[Partition.getPartitionId(key.digest)];
				assertSame(owner, Partition.write(cluster, wp, key).getNodeWrite(cluster));
				assertSame(owner, Partition.read(cluster, rp, key).getNodeRead(cluster));
				client.put(wp, key, new Bin("v", i));
				Record record = client.get(rp, key);
				assertEquals(i, record.getInt("v"));
				served.merge(owner.getName(), 1L, Long::sum);
				partitions.add(Partition.getPartitionId(key.digest));
			}

			for (L2Node node : nodes) {
				Map<String,Long> after = L2Topology.namespaceStats(node, stats);
				Map<String,Long> start = before.get(node.name);
				long writes = after.get("client_write_success") - start.get("client_write_success");
				long reads = after.get("client_read_success") - start.get("client_read_success");
				long proxied = after.get("client_proxy_complete") - start.get("client_proxy_complete") +
					after.get("client_proxy_error") - start.get("client_proxy_error") +
					after.get("client_proxy_timeout") - start.get("client_proxy_timeout");
				long expected = served.getOrDefault(node.name, 0L);
				System.out.println("L2-N5-SERVED node=" + node.name + " partitions=" + owners.get(node.name) +
					" keys=" + expected + " writes=" + writes + " reads=" + reads + " proxied=" + proxied);
				assertTrue(node.name + " served no keys", expected > 0);
				assertEquals(node.name + " writes", expected, writes);
				assertEquals(node.name + " reads", expected, reads);
				assertEquals(node.name + " proxied", 0, proxied);
			}
			System.out.println("L2-N5-KEYS keys=" + KEY_COUNT + " partitions=" + partitions.size());
		}
	}

	/**
	 * N-6: TLS through the proxy, ee only. The handshake validates the certificate against the
	 * node's tls-name, not the endpoint hostname the client connects to.
	 */
	@Test
	public void n6TlsValidatesAgainstNodeTlsName() throws Exception {
		skipUnless("ee".equals(L2Topology.subset),
			"N-6 needs DISCOVERY_SUBSET=ee for server TLS and TLS listeners; subset is " + L2Topology.subset);

		List<L2Node> nodes = L2Topology.nodes();

		for (L2Node node : nodes) {
			assertTrue("No TLS listener for " + node, node.tlsPort > 0);
			assertFalse("No advertised TLS address for " + node, node.tlsAdvertised.isEmpty());
			assertNotNull("No tls-name for " + node, node.tlsName);
		}

		L2Node first = nodes.get(0);
		assertFalse("tls-name " + first.tlsName + " is the endpoint hostname", first.tlsName.equals(L2Topology.host));

		ClientPolicy policy = L2Topology.policy();
		policy.tlsPolicy = new TlsPolicy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.tlsPortMap(nodes));

		try (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.host, first.tlsName, first.tlsPort))) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			L2Topology.assertAllPartitionsOwned("N-6", client);

			List<String> endpointAddresses = new ArrayList<>();

			for (InetAddress address : InetAddress.getAllByName(L2Topology.host)) {
				endpointAddresses.add(address.getHostAddress());
			}

			for (Node node : client.getNodes()) {
				Host h = node.getHost();
				L2Node expected = nodeOf(nodes, node.getName());
				System.out.println("L2-N6-NODE " + node.getName() + " host=" + h.name + ':' + h.port + " tlsName=" + h.tlsName +
					" endpoint=" + L2Topology.host + endpointAddresses);
				assertTrue(h.name + " is not " + L2Topology.host + endpointAddresses,
					h.name.equals(L2Topology.host) || endpointAddresses.contains(h.name));
				assertEquals(expected.tlsPort, h.port);
				assertEquals(expected.tlsName, h.tlsName);
			}

			for (int i = 0; i < 20; i++) {
				Key key = new Key(L2Topology.namespace, SET, "n6-" + i);
				client.put(null, key, new Bin("v", i));
				assertEquals(i, client.get(null, key).getInt("v"));
			}
		}

		try (AerospikeClient client = new AerospikeClient(policy, new Host(L2Topology.host, L2Topology.host, first.tlsPort))) {
			fail("N-6: TLS seed validated against endpoint hostname " + L2Topology.host);
		}
		catch (AerospikeException ae) {
			String message = messages(ae);
			System.out.println("L2-N6-ERROR " + message);
			assertTrue(message, message.contains("Invalid TLS name: " + L2Topology.host));
		}
	}

	/**
	 * N-7: node removed from the endpoint. With one node's listener disabled the client serves
	 * the other nodes' partitions; once it is enabled again the node returns and no node is stuck.
	 */
	@Test
	public void n7NodeRemovedFromEndpointDegradesAndReturns() throws Exception {
		List<L2Node> nodes = L2Topology.nodes();
		L2Node victim = nodes.get(nodes.size() - 1);
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(nodes));

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			Node[] masters = L2Topology.assertAllPartitionsOwned("N-7-before", client);
			WritePolicy wp = client.copyWritePolicyDefault();
			wp.maxRetries = 0;
			wp.totalTimeout = 2000;
			List<Key> victimKeys = new ArrayList<>();
			List<Key> otherKeys = new ArrayList<>();

			for (int i = 0; i < KEY_COUNT; i++) {
				Key key = new Key(L2Topology.namespace, SET, "n7-" + i);
				Node owner = masters[Partition.getPartitionId(key.digest)];
				(owner.getName().equals(victim.name) ? victimKeys : otherKeys).add(key);
			}
			assertFalse(victimKeys.isEmpty());
			assertFalse(otherKeys.isEmpty());

			disabledProxy = victim.proxy;
			L2Topology.setListener(victim.proxy, false);

			assertTrue("Writes to " + victim.name + " still succeed with its listener disabled",
				L2Topology.waitFor(() -> ! writes(client, wp, victimKeys.get(0))));

			int victimFailed = 0;

			for (Key key : victimKeys) {
				if (! writes(client, wp, key)) {
					victimFailed++;
				}
			}

			for (Key key : otherKeys) {
				client.put(wp, key, new Bin("v", 1));
				assertEquals(1, client.get(null, key).getInt("v"));
			}

			List<String> active = new ArrayList<>();

			for (Node node : client.getNodes()) {
				if (node.isActive() && ! node.getName().equals(victim.name)) {
					active.add(node.getName());
				}
			}
			active.sort(null);

			List<String> others = new ArrayList<>(L2Topology.nodeNames());
			others.remove(victim.name);
			System.out.println("L2-N7-DEGRADED victim=" + victim.name + " nodes=" + L2Topology.nodeNames(client) +
				" activeOthers=" + active + " otherKeysServed=" + otherKeys.size() + " victimKeysFailed=" + victimFailed +
				'/' + victimKeys.size());
			assertEquals(others, active);
			assertEquals(victimKeys.size(), victimFailed);

			L2Topology.setListener(victim.proxy, true);
			disabledProxy = null;

			assertTrue("Writes to " + victim.name + " did not recover",
				L2Topology.waitFor(() -> writes(client, wp, victimKeys.get(0))));
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			L2Topology.assertAllPartitionsOwned("N-7-after", client);

			for (Node node : client.getNodes()) {
				assertTrue(node.getName() + " is not active", node.isActive());
			}

			for (Key key : victimKeys) {
				client.put(wp, key, new Bin("v", 2));
				assertEquals(2, client.get(null, key).getInt("v"));
			}

			for (Key key : otherKeys) {
				client.put(wp, key, new Bin("v", 2));
				assertEquals(2, client.get(null, key).getInt("v"));
			}
			System.out.println("L2-N7-RECOVERED nodes=" + L2Topology.nodeNames(client) + " keys=" + KEY_COUNT);
		}
	}

	/**
	 * N-8: on_miss strict. A peer with no mapping produces the UX-2 error naming the node, the
	 * advertised endpoint and the translator, and creates no {@link Node}: its partitions have no
	 * master in the client's map. The other nodes keep serving every key and the missed peer is
	 * retried every tend.
	 */
	@Test
	public void n8StrictMissCreatesNoNode() throws Exception {
		List<L2Node> nodes = L2Topology.nodes();
		L2Node victim = nodes.get(nodes.size() - 1);
		List<L2Node> mapped = new ArrayList<>(nodes);
		mapped.remove(victim);
		List<String> mappedNames = new ArrayList<>();

		for (L2Node node : mapped) {
			mappedNames.add(node.name);
		}
		mappedNames.sort(null);

		warnings.clear();
		Log.setLevel(Log.Level.WARN);
		Log.setCallback((level, message) -> {
			if (level == Log.Level.WARN) {
				warnings.add(message);
			}
			System.out.println("CLIENT-LOG " + level + ' ' + message);
		});

		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(L2Topology.portMapByEndpoint(mapped));
		policy.translationOnMiss = TranslationMissMode.STRICT;

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, mappedNames);
			L2Topology.waitFor(() -> {
				assertEquals(mappedNames, L2Topology.nodeNames(client));
				return misses(victim).size() >= MISS_RETRIES;
			});

			List<String> misses = misses(victim);
			System.out.println("L2-N8-RESULT victim=" + victim.name + " nodes=" + L2Topology.nodeNames(client) +
				" missWarnings=" + misses.size());
			assertTrue("Missed peer " + victim.name + " not retried every tend: " + warnings, misses.size() >= MISS_RETRIES);
			assertEquals(mappedNames, L2Topology.nodeNames(client));

			String message = misses.get(0);
			System.out.println("L2-N8-LOG " + message);

			List<String> expected = new ArrayList<>();

			for (String advertised : victim.advertised) {
				int index = advertised.lastIndexOf(':');
				Endpoint endpoint = new Endpoint(victim.name, null, advertised.substring(0, index),
					Integer.parseInt(advertised.substring(index + 1)), Endpoint.SourceList.STANDARD);
				expected.add(new AerospikeException.TranslationMiss(endpoint, "port-map").getMessage());
			}
			assertTrue(message + " not in " + expected, expected.contains(message));

			for (String part : new String[] {victim.name, victim.advertised.get(0), "port-map", "pass_through"}) {
				assertTrue(part + " not in: " + message, message.contains(part));
			}
			assertOwnedKeysWork(client, mappedNames, victim);
		}
	}

	private static void assertPortMapJoins(String row, Map<String,String> portMap) throws Exception {
		ClientPolicy policy = L2Topology.policy();
		policy.addressTranslator = new PortMapAddressTranslator(portMap);
		System.out.println("L2-PORT-MAP " + row + ' ' + portMap);

		try (AerospikeClient client = new AerospikeClient(policy, L2Topology.seed())) {
			L2Topology.waitNodes(client, L2Topology.nodeNames());
			System.out.println("L2-JOIN " + row + " nodes=" + L2Topology.nodeNames(client));
			Map<String,Integer> owners = L2Topology.owners(L2Topology.assertAllPartitionsOwned(row, client));
			assertEquals(L2Topology.nodeNames(), new ArrayList<>(owners.keySet()));
		}
	}

	private static void assertOwnedKeysWork(AerospikeClient client, List<String> mappedNames, L2Node victim) {
		Partitions partitions = client.getCluster().partitionMap.get(L2Topology.namespace);
		AtomicReferenceArray<Node> masters = partitions.replicas[0];
		WritePolicy policy = client.copyWritePolicyDefault();
		int noMaster = 0;
		int owned = 0;
		int missedKeys = 0;

		for (int i = 0; i < Node.PARTITIONS; i++) {
			Node node = masters.get(i);

			if (node == null) {
				noMaster++;
			}
			else {
				assertTrue(node.getName(), mappedNames.contains(node.getName()));
			}
		}

		for (int i = 0; i < KEY_COUNT; i++) {
			Key key = new Key(L2Topology.namespace, SET, "n8-" + i);

			if (masters.get(Partition.getPartitionId(key.digest)) == null) {
				missedKeys++;
			}
			else {
				owned++;
			}
			client.put(policy, key, new Bin("v", i));
			assertEquals(i, client.get(null, key).getInt("v"));
		}
		System.out.println("L2-N8-KEYS victim=" + victim.name + " partitionsWithoutMaster=" + noMaster +
			" keysOnMappedMasters=" + owned + " keysOnMissedPartitions=" + missedKeys);
		assertTrue(noMaster > 0);
		assertTrue(owned > 0);
		assertTrue(missedKeys > 0);
	}

	private static List<String> misses(L2Node victim) {
		List<String> misses = new ArrayList<>();

		for (String warning : warnings) {
			if (warning.contains("has no mapping for node " + victim.name)) {
				misses.add(warning);
			}
		}
		return misses;
	}

	private static boolean writes(AerospikeClient client, WritePolicy policy, Key key) {
		try {
			client.put(policy, key, new Bin("v", 0));
			return true;
		}
		catch (AerospikeException ae) {
			return false;
		}
	}

	private static void skipUnless(boolean condition, String reason) {
		if (! condition) {
			System.out.println("L2-SKIP " + reason);
		}
		Assume.assumeTrue(reason, condition);
	}

	private static L2Node nodeOf(List<L2Node> nodes, String name) {
		for (L2Node node : nodes) {
			if (node.name.equals(name)) {
				return node;
			}
		}
		throw new IllegalStateException("Unknown node " + name);
	}

	private static String messages(Throwable t) {
		StringBuilder sb = new StringBuilder();

		for (Throwable e = t; e != null; e = e.getCause()) {
			if (sb.length() > 0) {
				sb.append(" <- ");
			}
			sb.append(e.getMessage());
		}
		return sb.toString();
	}
}
